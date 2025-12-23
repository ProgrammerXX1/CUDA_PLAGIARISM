#include <cstdlib>
#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <filesystem>
#include <chrono>

#include <dlfcn.h>
#include <pqxx/pqxx>
#include <nlohmann/json.hpp>

#include "httplib.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

// ---------------- env ----------------
static std::string env_or(const char* k, const std::string& defv) {
    const char* v = std::getenv(k);
    return (v && *v) ? std::string(v) : defv;
}
static std::string env_req(const char* k) {
    const char* v = std::getenv(k);
    if (!v || !*v) throw std::runtime_error(std::string("missing env: ") + k);
    return std::string(v);
}
static std::string pg_conninfo_from_env() {
    std::ostringstream ss;
    ss << "host=" << env_req("PG_HOST")
       << " port=" << env_or("PG_PORT", "5432")
       << " dbname=" << env_req("PG_DB")
       << " user=" << env_req("PG_USER")
       << " password=" << env_req("PG_PASS");
    return ss.str();
}

// ---------------- util ----------------
static json parse_json_body(const httplib::Request& req) {
    if (req.body.empty()) return json::object();
    return json::parse(req.body);
}
static std::string now_version_tag() {
    using namespace std::chrono;
    auto t = system_clock::now();
    auto s = duration_cast<seconds>(t.time_since_epoch()).count();
    return "v" + std::to_string(s);
}

// ---------------- DB ops ----------------
// upsert doc by doc_id
static json db_upsert_doc(const json& body) {
    const std::string doc_id = body.value("doc_id", "");
    const std::string text   = body.value("text", "");
    const std::string title  = body.value("title", "");
    const std::string author = body.value("author", "");
    json meta = body.value("meta", json::object());

    if (doc_id.empty()) throw std::runtime_error("doc_id is required");
    if (text.empty()) throw std::runtime_error("text is required");

    pqxx::connection c(pg_conninfo_from_env());
    pqxx::work tx(c);

    // parameterized to avoid injection
    tx.exec_params(
        "INSERT INTO core_documents (doc_id, title, author, text_content, meta_json, status) "
        "VALUES ($1,$2,$3,$4,$5::jsonb,'stored') "
        "ON CONFLICT (doc_id) DO UPDATE SET "
        "  title=EXCLUDED.title, "
        "  author=EXCLUDED.author, "
        "  text_content=EXCLUDED.text_content, "
        "  meta_json=EXCLUDED.meta_json, "
        "  status='stored'",
        doc_id, title, author, text, meta.dump()
    );

    tx.commit();

    return json{{"ok", true}, {"doc_id", doc_id}};
}

// build corpus.jsonl from DB
static json db_build_corpus(const json& body) {
    fs::path corpus_path = env_req("CORPUS_JSONL");
    if (body.contains("corpus_path") && body["corpus_path"].is_string()) {
        corpus_path = body["corpus_path"].get<std::string>();
    }
    fs::create_directories(corpus_path.parent_path());

    pqxx::connection c(pg_conninfo_from_env());
    pqxx::work tx(c);

    // берём stored + indexed (можно только stored)
    auto r = tx.exec(
        "SELECT doc_id, COALESCE(title,''), COALESCE(author,''), text_content "
        "FROM core_documents "
        "WHERE status IN ('stored','indexed') "
        "ORDER BY id"
    );

    std::ofstream out(corpus_path, std::ios::binary);
    if (!out) throw std::runtime_error("cannot write corpus: " + corpus_path.string());

    int written = 0;
    for (auto row : r) {
        const std::string doc_id = row[0].as<std::string>();
        const std::string title  = row[1].as<std::string>();
        const std::string author = row[2].as<std::string>();
        const std::string text   = row[3].as<std::string>();

        if (doc_id.empty() || text.empty()) continue;

        json rec{
            {"doc_id", doc_id},
            {"text", text},
            {"title", title},
            {"author", author}
        };
        out << rec.dump() << "\n";
        written++;
    }

    tx.commit();

    return json{{"ok", true}, {"corpus_path", corpus_path.string()}, {"corpus_docs", written}};
}

// ---------------- index_builder runner ----------------
static json run_index_builder(const json& body) {
    fs::path corpus_path = env_req("CORPUS_JSONL");
    if (body.contains("corpus_path")) corpus_path = body["corpus_path"].get<std::string>();

    fs::path index_root = env_req("INDEX_ROOT");
    std::string version = body.value("version", "");
    if (version.empty()) version = now_version_tag();

    fs::path index_dir = index_root / version;
    fs::create_directories(index_dir);

    const fs::path bin = env_req("INDEX_BUILDER_PATH");
    if (!fs::exists(bin)) throw std::runtime_error("INDEX_BUILDER_PATH not found: " + bin.string());
    if (!fs::exists(corpus_path)) throw std::runtime_error("corpus not found: " + corpus_path.string());

    // logs
    fs::path outlog = index_dir / "build.stdout.log";
    fs::path errlog = index_dir / "build.stderr.log";

    std::ostringstream cmd;
    cmd << bin.string() << " " << corpus_path.string() << " " << index_dir.string()
        << " > " << outlog.string()
        << " 2> " << errlog.string();

    int rc = std::system(cmd.str().c_str());

    // save version in DB
    pqxx::connection c(pg_conninfo_from_env());
    pqxx::work tx(c);
    tx.exec_params(
        "INSERT INTO core_index_versions (version, index_dir, corpus_path, status, stats_json) "
        "VALUES ($1,$2,$3,$4,$5::jsonb) "
        "ON CONFLICT (version) DO UPDATE SET "
        "  index_dir=EXCLUDED.index_dir, corpus_path=EXCLUDED.corpus_path, status=EXCLUDED.status, stats_json=EXCLUDED.stats_json",
        version,
        index_dir.string(),
        corpus_path.string(),
        (rc == 0 ? "built" : "failed"),
        json{{"rc", rc}}.dump()
    );
    tx.commit();

    return json{
        {"ok", rc == 0},
        {"rc", rc},
        {"version", version},
        {"index_dir", index_dir.string()},
        {"stdout_log", outlog.string()},
        {"stderr_log", errlog.string()}
    };
}

// ---------------- libsearchcore.so bindings ----------------
struct SeHit {
    int    doc_id_int;
    double score;
    double j9;
    double c9;
    double j13;
    double c13;
    int    cand_hits;
};
struct SeSearchResult { int count; };

using fn_se_load_index  = int(*)(const char*);
using fn_se_search_text = SeSearchResult(*)(const char*, int, SeHit*, int);

static void* g_lib = nullptr;
static fn_se_load_index  g_load = nullptr;
static fn_se_search_text g_search = nullptr;

static bool g_loaded = false;
static fs::path g_current_index_dir;
static std::vector<std::string> g_doc_ids;

static std::string read_file(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) return {};
    std::ostringstream ss; ss << f.rdbuf();
    return ss.str();
}

static void ensure_core_loaded() {
    if (g_lib) return;

    const std::string so = env_req("LIBSEARCHCORE_PATH");
    g_lib = dlopen(so.c_str(), RTLD_NOW);
    if (!g_lib) throw std::runtime_error(std::string("dlopen failed: ") + dlerror());

    g_load = (fn_se_load_index)dlsym(g_lib, "se_load_index");
    g_search = (fn_se_search_text)dlsym(g_lib, "se_search_text");
    if (!g_load || !g_search) throw std::runtime_error("dlsym failed: missing se_load_index/se_search_text");
}

static void load_docids(const fs::path& index_dir) {
    fs::path p = index_dir / "index_native_docids.json";
    std::string s = read_file(p);
    if (s.empty()) throw std::runtime_error("missing index_native_docids.json in " + index_dir.string());
    g_doc_ids = json::parse(s).get<std::vector<std::string>>();
}

static json api_index_load(const json& body) {
    ensure_core_loaded();

    fs::path index_dir;
    if (body.contains("index_dir")) {
        index_dir = body["index_dir"].get<std::string>();
    } else {
        // если не передали — берём current из БД
        pqxx::connection c(pg_conninfo_from_env());
        pqxx::work tx(c);
        auto r = tx.exec("SELECT COALESCE(current_index_dir,'') FROM core_runtime_state WHERE id=1");
        std::string cur = (r.size() ? r[0][0].as<std::string>() : "");
        tx.commit();
        if (cur.empty()) throw std::runtime_error("no current_index_dir in core_runtime_state, call /v1/index/set_current");
        index_dir = cur;
    }

    int rc = g_load(index_dir.string().c_str());
    if (rc != 0) throw std::runtime_error("se_load_index failed rc=" + std::to_string(rc));

    load_docids(index_dir);

    g_current_index_dir = index_dir;
    g_loaded = true;

    return json{{"ok", true}, {"index_dir", index_dir.string()}, {"doc_ids", (int)g_doc_ids.size()}};
}

static json api_search(const json& body) {
    if (!g_loaded) throw std::runtime_error("index not loaded");

    std::string q = body.value("q", "");
    int top = body.value("top", 10);
    if (q.empty()) return json{{"hits_total", 0}, {"documents", json::array()}};

    constexpr int MAX_HITS = 4096;
    std::vector<SeHit> hits(MAX_HITS);

    SeSearchResult r = g_search(q.c_str(), top, hits.data(), MAX_HITS);
    int n = r.count;

    json docs = json::array();
    for (int i = 0; i < n; ++i) {
        int di = hits[i].doc_id_int;
        if (di < 0 || di >= (int)g_doc_ids.size()) continue;

        docs.push_back(json{
            {"doc_id", g_doc_ids[di]},
            {"score", hits[i].score},
            {"J9", hits[i].j9},
            {"C9", hits[i].c9},
            {"J13", hits[i].j13},
            {"C13", hits[i].c13},
            {"cand_hits", hits[i].cand_hits}
        });
    }

    return json{{"hits_total", (int)docs.size()}, {"documents", docs}};
}

// set current index dir in DB (atomic pointer)
static json api_set_current(const json& body) {
    const std::string index_dir = body.value("index_dir", "");
    const std::string version   = body.value("version", "");

    if (index_dir.empty()) throw std::runtime_error("index_dir required");
    if (!fs::exists(index_dir)) throw std::runtime_error("index_dir does not exist: " + index_dir);

    pqxx::connection c(pg_conninfo_from_env());
    pqxx::work tx(c);
    tx.exec_params(
        "UPDATE core_runtime_state SET current_version=$1, current_index_dir=$2 WHERE id=1",
        version, index_dir
    );
    tx.commit();

    return json{{"ok", true}, {"current_version", version}, {"current_index_dir", index_dir}};
}

// ---------------- HTTP ----------------
int main() {
    httplib::Server svr;

    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("ok", "text/plain; charset=utf-8");
    });

    auto ok = [](httplib::Response& res, const json& j) {
        res.set_content(j.dump(), "application/json; charset=utf-8");
    };
    auto fail = [](httplib::Response& res, const std::string& msg) {
        res.status = 400;
        res.set_content(json{{"ok", false}, {"error", msg}}.dump(), "application/json; charset=utf-8");
    };

    // 1) ingest text
    svr.Post("/v1/docs/upsert", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, db_upsert_doc(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    // 2) build corpus from DB
    svr.Post("/v1/corpus/build", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, db_build_corpus(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    // 3) build index from corpus
    svr.Post("/v1/index/build", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, run_index_builder(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    // 3.5) convenience: rebuild = corpus + build index
    svr.Post("/v1/index/rebuild", [&](const httplib::Request& req, httplib::Response& res) {
        try {
            json body = parse_json_body(req);
            json c = db_build_corpus(body);
            json b = run_index_builder(body);
            ok(res, json{{"ok", true}, {"corpus", c}, {"build", b}});
        } catch (const std::exception& e) {
            fail(res, e.what());
        }
    });

    // 4) set current index dir in DB
    svr.Post("/v1/index/set_current", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, api_set_current(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    // 5) load current (or explicit) index into memory
    svr.Post("/v1/index/load", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, api_index_load(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    // 6) search
    svr.Post("/v1/search", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, api_search(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    const std::string host = env_or("HOST", "0.0.0.0");
    const int port = std::stoi(env_or("PORT", "8080"));

    std::cout << "listening on http://" << host << ":" << port << std::endl;
    if (!svr.listen(host.c_str(), port)) return 1;
    return 0;
}
