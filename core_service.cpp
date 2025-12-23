#include <cstdlib>
#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <optional>
#include <filesystem>

#include <dlfcn.h>
#include <pqxx/pqxx>
#include <nlohmann/json.hpp>

#include "httplib.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

// ---------------- env helpers ----------------
static std::string env_or(const char* k, const std::string& defv) {
    const char* v = std::getenv(k);
    return (v && *v) ? std::string(v) : defv;
}
static std::string env_req(const char* k) {
    const char* v = std::getenv(k);
    if (!v || !*v) throw std::runtime_error(std::string("missing env: ") + k);
    return std::string(v);
}

// ---------------- util ----------------
static std::string read_file_or_empty(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) return {};
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

static bool ends_with(const std::string& s, const char* suf) {
    size_t n = s.size(), m = std::strlen(suf);
    if (m > n) return false;
    return std::memcmp(s.data() + (n - m), suf, m) == 0;
}

// Упрощённый extract: читаем как UTF-8 текст только .txt
static std::optional<std::string> extract_text_best_effort(const fs::path& file_path) {
    const std::string p = file_path.string();
    if (!ends_with(p, ".txt")) {
        return std::nullopt; // позже сюда подключим нормальный PDF/DOCX extract
    }
    std::string body = read_file_or_empty(file_path);
    if (body.empty()) return std::nullopt;
    return body;
}

// ---------------- Postgres: rebuild corpus ----------------
struct DocRow {
    int id;
    std::string external_id;
    std::string title;
    std::string student_name;
};

static std::string pg_conninfo_from_env() {
    // PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS
    std::string host = env_req("PG_HOST");
    std::string port = env_or("PG_PORT", "5432");
    std::string db   = env_req("PG_DB");
    std::string user = env_req("PG_USER");
    std::string pass = env_req("PG_PASS");

    std::ostringstream ss;
    ss << "host=" << host
       << " port=" << port
       << " dbname=" << db
       << " user=" << user
       << " password=" << pass;
    return ss.str();
}

static std::vector<DocRow> fetch_docs_l5_uploaded(
    pqxx::connection& c,
    const std::vector<int>& only_doc_ids
) {
    pqxx::work tx(c);

    std::vector<DocRow> out;

    if (only_doc_ids.empty()) {
        auto r = tx.exec(
            "SELECT id, COALESCE(external_id,''), COALESCE(title,''), COALESCE(student_name,'') "
            "FROM document WHERE status='l5_uploaded'"
        );
        out.reserve(r.size());
        for (auto row : r) {
            DocRow d;
            d.id = row[0].as<int>();
            d.external_id = row[1].as<std::string>();
            d.title = row[2].as<std::string>();
            d.student_name = row[3].as<std::string>();
            out.push_back(std::move(d));
        }
    } else {
        // простая сборка IN (...) (для MVP)
        std::ostringstream in;
        for (size_t i = 0; i < only_doc_ids.size(); ++i) {
            if (i) in << ",";
            in << only_doc_ids[i];
        }

        std::string q =
            "SELECT id, COALESCE(external_id,''), COALESCE(title,''), COALESCE(student_name,'') "
            "FROM document WHERE status='l5_uploaded' AND id IN (" + in.str() + ")";

        auto r = tx.exec(q);
        out.reserve(r.size());
        for (auto row : r) {
            DocRow d;
            d.id = row[0].as<int>();
            d.external_id = row[1].as<std::string>();
            d.title = row[2].as<std::string>();
            d.student_name = row[3].as<std::string>();
            out.push_back(std::move(d));
        }
    }

    tx.commit();
    return out;
}

static json rebuild_l5_corpus_cpp(const json& body) {
    // env:
    // UPLOAD_DIR=/path/to/uploads
    // CORPUS_JSONL=/path/to/corpus.jsonl
    const fs::path upload_dir = env_req("UPLOAD_DIR");
    fs::path corpus_path = env_req("CORPUS_JSONL");

    std::vector<int> only_ids;
    if (body.contains("only_doc_ids") && body["only_doc_ids"].is_array()) {
        for (auto& x : body["only_doc_ids"]) only_ids.push_back(x.get<int>());
    }
    if (body.contains("corpus_path") && body["corpus_path"].is_string()) {
        corpus_path = body["corpus_path"].get<std::string>();
    }

    pqxx::connection c(pg_conninfo_from_env());
    auto docs = fetch_docs_l5_uploaded(c, only_ids);

    if (docs.empty()) {
        return json{{"docs_total", 0}, {"corpus_docs", 0}, {"corpus_path", corpus_path.string()}};
    }

    fs::create_directories(corpus_path.parent_path());

    int written = 0;
    int skipped_no_external = 0;
    int skipped_no_file = 0;
    int skipped_no_text = 0;

    std::ofstream out(corpus_path, std::ios::binary);
    if (!out) throw std::runtime_error("cannot write corpus: " + corpus_path.string());

    for (const auto& d : docs) {
        if (d.external_id.empty()) { skipped_no_external++; continue; }

        fs::path file_path = upload_dir / d.external_id;
        if (!fs::exists(file_path)) { skipped_no_file++; continue; }

        auto maybe_text = extract_text_best_effort(file_path);
        if (!maybe_text.has_value() || maybe_text->empty()) { skipped_no_text++; continue; }

        // ВАЖНО: doc_id должен быть тем, что тебе нужно видеть в поиске.
        // Сейчас ставим doc_id = external_id (обычно это внешний document_id).
        json rec{
            {"doc_id", d.external_id},
            {"text", *maybe_text},
            {"title", d.title},
            {"author", d.student_name}
        };
        out << rec.dump() << "\n";
        written++;
    }

    return json{
        {"docs_total", (int)docs.size()},
        {"corpus_docs", written},
        {"corpus_path", corpus_path.string()},
        {"skipped_no_external", skipped_no_external},
        {"skipped_no_file", skipped_no_file},
        {"skipped_no_text", skipped_no_text}
    };
}

// ---------------- index build: run index_builder ----------------
static fs::path find_index_builder() {
    // можно задать INDEX_BUILDER_PATH
    const std::string envp = env_or("INDEX_BUILDER_PATH", "");
    if (!envp.empty() && fs::exists(envp)) return fs::path(envp);

    std::vector<fs::path> candidates = {
        "/usr/local/bin/index_builder",
        fs::current_path() / "index_builder",
        fs::current_path() / "build" / "index_builder",
    };
    for (auto& p : candidates) if (fs::exists(p)) return p;
    throw std::runtime_error("index_builder not found (set INDEX_BUILDER_PATH)");
}

static json build_index_cpp(const json& body) {
    fs::path corpus = env_req("CORPUS_JSONL");
    fs::path index_dir = env_req("INDEX_DIR");

    if (body.contains("corpus_path")) corpus = body["corpus_path"].get<std::string>();
    if (body.contains("index_dir")) index_dir = body["index_dir"].get<std::string>();

    if (!fs::exists(corpus)) throw std::runtime_error("corpus not found: " + corpus.string());
    fs::create_directories(index_dir);

    fs::path bin = find_index_builder();

    std::ostringstream cmd;
    cmd << bin.string() << " " << corpus.string() << " " << index_dir.string()
        << " > " << (index_dir / "build.stdout.log").string()
        << " 2> " << (index_dir / "build.stderr.log").string();

    int rc = std::system(cmd.str().c_str());
    return json{
        {"rc", rc},
        {"corpus_path", corpus.string()},
        {"index_dir", index_dir.string()},
        {"stdout_log", (index_dir / "build.stdout.log").string()},
        {"stderr_log", (index_dir / "build.stderr.log").string()}
    };
}

// ---------------- searchcore.so: load + search ----------------
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

static bool g_index_loaded = false;
static fs::path g_current_index_dir;
static std::vector<std::string> g_doc_ids;  // index_native_docids.json

static void ensure_lib_loaded() {
    if (g_lib) return;

    // LIBSEARCHCORE_PATH=/usr/local/lib/libsearchcore.so
    const std::string so = env_req("LIBSEARCHCORE_PATH");
    g_lib = dlopen(so.c_str(), RTLD_NOW);
    if (!g_lib) throw std::runtime_error(std::string("dlopen failed: ") + dlerror());

    g_load = (fn_se_load_index)dlsym(g_lib, "se_load_index");
    g_search = (fn_se_search_text)dlsym(g_lib, "se_search_text");
    if (!g_load || !g_search) throw std::runtime_error("dlsym failed: missing symbols");
}

static void load_doc_ids_json(const fs::path& index_dir) {
    auto p = index_dir / "index_native_docids.json";
    std::string s = read_file_or_empty(p);
    if (s.empty()) throw std::runtime_error("missing docids: " + p.string());
    g_doc_ids = json::parse(s).get<std::vector<std::string>>();
}

static json api_index_load(const json& body) {
    ensure_lib_loaded();

    fs::path index_dir = env_req("INDEX_DIR");
    if (body.contains("index_dir")) index_dir = body["index_dir"].get<std::string>();

    int rc = g_load(index_dir.string().c_str());
    if (rc != 0) throw std::runtime_error("se_load_index failed rc=" + std::to_string(rc));

    load_doc_ids_json(index_dir);

    g_current_index_dir = index_dir;
    g_index_loaded = true;

    return json{{"ok", true}, {"index_dir", index_dir.string()}, {"doc_ids", (int)g_doc_ids.size()}};
}

static json api_search(const json& body) {
    if (!g_index_loaded) throw std::runtime_error("index not loaded");

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

// ---------------- HTTP server ----------------
static json parse_json_body(const httplib::Request& req) {
    if (req.body.empty()) return json::object();
    return json::parse(req.body);
}

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

    svr.Post("/v1/l5/corpus/rebuild", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, rebuild_l5_corpus_cpp(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    svr.Post("/v1/l5/index/build", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, build_index_cpp(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    svr.Post("/v1/l5/index/load", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, api_index_load(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    svr.Post("/v1/l5/search", [&](const httplib::Request& req, httplib::Response& res) {
        try { ok(res, api_search(parse_json_body(req))); }
        catch (const std::exception& e) { fail(res, e.what()); }
    });

    const std::string host = env_or("HOST", "0.0.0.0");
    const int port = std::stoi(env_or("PORT", "8080"));

    std::cout << "listening on http://" << host << ":" << port << std::endl;
    if (!svr.listen(host.c_str(), port)) return 1;
    return 0;
}
