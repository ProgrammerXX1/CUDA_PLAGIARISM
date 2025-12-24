#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cstdint>
#include <algorithm>
#include <filesystem>

#include <nlohmann/json.hpp>
#include "text_common.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

constexpr int K = 9;
constexpr std::uint32_t MAX_TOKENS_PER_DOC   = 100000;  // 0 = без лимита
constexpr std::uint32_t MAX_SHINGLES_PER_DOC = 50000;   // 0 = без лимита
constexpr int SHINGLE_STRIDE = 1;

struct DocMeta {
    std::uint32_t tok_len;
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

struct DocInfo {
    std::string doc_id;
    std::string title;
    std::string author;
};

static bool parse_line_json(const std::string& line, DocInfo& info, std::string& text) {
    try {
        auto j = json::parse(line);
        if (!j.is_object()) return false;

        info.doc_id = j.value("doc_id", ""); // ВАЖНО: doc_id (а не "doc_id"/"document_id" вперемешку)
        if (info.doc_id.empty()) return false;

        text = j.value("text", "");
        if (text.empty()) return false;

        info.title  = j.value("title", "");
        info.author = j.value("author", "");
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: index_builder <corpus_jsonl> <out_dir>\n";
        return 1;
    }

    const fs::path corpus_path = argv[1];
    const fs::path out_dir     = argv[2];

    std::ifstream in(corpus_path);
    if (!in) {
        std::cerr << "cannot open " << corpus_path << "\n";
        return 1;
    }
    fs::create_directories(out_dir);

    std::vector<DocMeta> docs;
    std::vector<DocInfo> infos;
    std::vector<std::pair<std::uint64_t, std::uint32_t>> postings9;

    docs.reserve(1024);
    infos.reserve(1024);
    postings9.reserve(1024 * 64);

    std::vector<TokenSpan> spans;
    spans.reserve(256);

    std::uint64_t skipped_bad_json = 0;
    std::uint64_t skipped_bad_doc  = 0;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        DocInfo info;
        std::string text;
        if (!parse_line_json(line, info, text)) {
            skipped_bad_json++;
            continue;
        }

        std::string norm = normalize_for_shingles_simple(text);

        tokenize_spans(norm, spans);
        if (spans.empty()) { skipped_bad_doc++; continue; }

        if (MAX_TOKENS_PER_DOC > 0 && spans.size() > (std::size_t)MAX_TOKENS_PER_DOC)
            spans.resize(MAX_TOKENS_PER_DOC);

        if (spans.size() < (std::size_t)K) { skipped_bad_doc++; continue; }

        const int n   = (int)spans.size();
        const int cnt = n - K + 1;
        if (cnt <= 0) { skipped_bad_doc++; continue; }

        auto [hi, lo] = simhash128_spans(norm, spans);

        DocMeta dm{};
        dm.tok_len    = (std::uint32_t)spans.size();
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;

        const std::uint32_t doc_idx = (std::uint32_t)docs.size();
        docs.push_back(dm);
        infos.push_back(std::move(info));

        const int step = (SHINGLE_STRIDE > 0 ? SHINGLE_STRIDE : 1);
        std::uint32_t produced = 0;
        const std::uint32_t max_sh =
            (MAX_SHINGLES_PER_DOC > 0) ? MAX_SHINGLES_PER_DOC : (std::uint32_t)cnt;

        for (int pos = 0; pos < cnt && produced < max_sh; pos += step) {
            std::uint64_t h = hash_shingle_tokens_spans(norm, spans, pos, K);
            postings9.emplace_back(h, doc_idx);
            ++produced;
        }
    }

    const std::uint32_t N_docs = (std::uint32_t)docs.size();
    if (N_docs == 0) {
        std::cerr << "no valid docs. skipped_bad_json=" << skipped_bad_json
                  << " skipped_bad_doc=" << skipped_bad_doc << "\n";
        return 1;
    }

    std::sort(postings9.begin(), postings9.end(),
              [](const auto& a, const auto& b) {
                  if (a.first < b.first) return true;
                  if (a.first > b.first) return false;
                  return a.second < b.second;
              });

    const std::uint64_t N_post9  = (std::uint64_t)postings9.size();
    const std::uint64_t N_post13 = 0;

    // ---- write index_native.bin
    {
        const fs::path bin_path = out_dir / "index_native.bin";
        std::ofstream bout(bin_path, std::ios::binary);
        if (!bout) {
            std::cerr << "cannot open " << bin_path << " for write\n";
            return 1;
        }

        const char magic[4] = {'P','L','A','G'};
        std::uint32_t version = 1;

        bout.write(magic, 4);
        bout.write((const char*)&version, sizeof(version));
        bout.write((const char*)&N_docs,  sizeof(N_docs));
        bout.write((const char*)&N_post9, sizeof(N_post9));
        bout.write((const char*)&N_post13,sizeof(N_post13));

        for (const auto& dm : docs) {
            bout.write((const char*)&dm.tok_len,    sizeof(dm.tok_len));
            bout.write((const char*)&dm.simhash_hi, sizeof(dm.simhash_hi));
            bout.write((const char*)&dm.simhash_lo, sizeof(dm.simhash_lo));
        }

        for (const auto& p : postings9) {
            const std::uint64_t h = p.first;
            const std::uint32_t d = p.second;
            bout.write((const char*)&h, sizeof(h));
            bout.write((const char*)&d, sizeof(d));
        }
    }

    // ---- write index_native_docids.json
    {
        std::vector<std::string> doc_ids;
        doc_ids.reserve(infos.size());
        for (auto& x : infos) doc_ids.push_back(x.doc_id);

        const fs::path p = out_dir / "index_native_docids.json";
        std::ofstream f(p);
        if (!f) {
            std::cerr << "cannot open " << p << " for write\n";
            return 1;
        }
        f << json(doc_ids).dump();
    }

    // ---- write index_native_meta.json
    {
        json docs_meta = json::object();
        for (std::size_t i = 0; i < infos.size(); ++i) {
            const auto& info = infos[i];
            const auto& dm   = docs[i];

            json m;
            m["tok_len"]    = dm.tok_len;
            m["simhash_hi"] = dm.simhash_hi;
            m["simhash_lo"] = dm.simhash_lo;
            if (!info.title.empty())  m["title"]  = info.title;
            if (!info.author.empty()) m["author"] = info.author;

            docs_meta[info.doc_id] = std::move(m);
        }

        json meta;
        meta["docs_meta"] = std::move(docs_meta);
        meta["config"] = {
            {"thresholds", {{"plag_thr", 0.7}, {"partial_thr", 0.3}}}
        };
        meta["stats"] = {{"docs", N_docs}, {"k9", N_post9}, {"k13", 0}};

        const fs::path p = out_dir / "index_native_meta.json";
        std::ofstream f(p);
        if (!f) {
            std::cerr << "cannot open " << p << " for write\n";
            return 1;
        }
        f << meta.dump();
    }

    std::cout << "[index_builder] ok docs=" << N_docs
              << " post9=" << N_post9
              << " skipped_bad_json=" << skipped_bad_json
              << " skipped_bad_doc=" << skipped_bad_doc
              << " out_dir=" << out_dir << "\n";
    return 0;
}
