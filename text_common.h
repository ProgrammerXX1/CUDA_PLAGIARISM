#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <utility>
#include <cctype>
#include <cstring>

struct TokenSpan {
    std::uint32_t start = 0;
    std::uint32_t len   = 0;
};

// Нормализация (MVP):
// - ASCII -> lower
// - все ASCII не [a-z0-9] превращаем в пробел
// - байты >=128 оставляем как есть (UTF-8 без lower, иначе нужен ICU)
inline std::string normalize_for_shingles_simple(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    bool prev_space = true;

    for (unsigned char ch : s) {
        if (ch < 128) {
            unsigned char c = (unsigned char)std::tolower(ch);
            bool ok = (std::isalnum(c) != 0);
            if (ok) {
                out.push_back((char)c);
                prev_space = false;
            } else {
                if (!prev_space) out.push_back(' ');
                prev_space = true;
            }
        } else {
            out.push_back((char)ch);
            prev_space = false;
        }
    }
    while (!out.empty() && out.back() == ' ') out.pop_back();
    return out;
}

inline void tokenize_spans(const std::string& norm, std::vector<TokenSpan>& spans) {
    spans.clear();
    const std::uint32_t n = (std::uint32_t)norm.size();
    std::uint32_t i = 0;

    while (i < n) {
        while (i < n && norm[i] == ' ') i++;
        if (i >= n) break;

        std::uint32_t j = i;
        while (j < n && norm[j] != ' ') j++;

        spans.push_back(TokenSpan{i, (std::uint32_t)(j - i)});
        i = j;
    }
}

inline std::uint64_t fnv1a64(const void* data, std::size_t n) {
    const unsigned char* p = (const unsigned char*)data;
    std::uint64_t h = 1469598103934665603ull;
    for (std::size_t i = 0; i < n; ++i) {
        h ^= (std::uint64_t)p[i];
        h *= 1099511628211ull;
    }
    return h;
}

inline std::uint64_t mix64(std::uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ull;
    x ^= x >> 33;
    return x;
}

inline std::uint64_t hash_shingle_tokens_spans(
    const std::string& norm,
    const std::vector<TokenSpan>& spans,
    int pos,
    int K
) {
    std::uint64_t h = 1469598103934665603ull;
    for (int i = 0; i < K; ++i) {
        const auto& t = spans[pos + i];
        std::uint64_t th = fnv1a64(norm.data() + t.start, t.len);
        h ^= th; h *= 1099511628211ull;
        h ^= 0x0Au; h *= 1099511628211ull; // '\n'
    }
    return h;
}

inline std::pair<std::uint64_t, std::uint64_t> simhash128_spans(
    const std::string& norm,
    const std::vector<TokenSpan>& spans
) {
    int acc1[64] = {0};
    int acc2[64] = {0};

    for (const auto& t : spans) {
        const char* p = norm.data() + t.start;
        std::size_t n = t.len;

        std::uint64_t h1 = fnv1a64(p, n);
        std::uint64_t h2 = mix64(h1 ^ 0x9e3779b97f4a7c15ull);

        for (int b = 0; b < 64; ++b) {
            acc1[b] += ((h1 >> b) & 1ull) ? 1 : -1;
            acc2[b] += ((h2 >> b) & 1ull) ? 1 : -1;
        }
    }

    std::uint64_t hi = 0, lo = 0;
    for (int b = 0; b < 64; ++b) {
        if (acc1[b] >= 0) hi |= (1ull << b);
        if (acc2[b] >= 0) lo |= (1ull << b);
    }
    return {hi, lo};
}
