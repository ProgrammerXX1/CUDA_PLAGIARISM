#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>

#include "httplib.h"

static std::string read_file_or_empty(const std::filesystem::path& p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) return {};
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

static const char* guess_mime(const std::string& path) {
    if (path.ends_with(".html")) return "text/html; charset=utf-8";
    if (path.ends_with(".css"))  return "text/css; charset=utf-8";
    if (path.ends_with(".js"))   return "application/javascript; charset=utf-8";
    if (path.ends_with(".png"))  return "image/png";
    if (path.ends_with(".svg"))  return "image/svg+xml";
    if (path.ends_with(".json")) return "application/json; charset=utf-8";
    if (path.ends_with(".map"))  return "application/json; charset=utf-8";
    return "application/octet-stream";
}

int main() {
    httplib::Server svr;

    // hello endpoints
    svr.Get("/", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("hello world", "text/plain; charset=utf-8");
    });

    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("ok", "text/plain; charset=utf-8");
    });

    svr.Post("/echo", [](const httplib::Request& req, httplib::Response& res) {
        res.set_content(req.body, "text/plain; charset=utf-8");
    });

    // OpenAPI spec
    svr.Get("/openapi.json", [](const httplib::Request&, httplib::Response& res) {
        std::string body = read_file_or_empty("openapi.json");
        if (body.empty()) {
            res.status = 404;
            res.set_content("openapi.json not found", "text/plain; charset=utf-8");
            return;
        }
        res.set_content(body, "application/json; charset=utf-8");
    });

    // /docs -> /docs/
    svr.Get("/docs", [](const httplib::Request&, httplib::Response& res) {
        res.status = 302;
        res.set_header("Location", "/docs/");
    });

    // Swagger UI static files: /docs/* from ./swagger-ui/
    svr.Get(R"(/docs/(.*))", [](const httplib::Request& req, httplib::Response& res) {
        // req.matches[1] = everything after /docs/
        std::string rel = req.matches[1].str();
        if (rel.empty()) rel = "index.html";

        // protect from path traversal
        if (rel.find("..") != std::string::npos) {
            res.status = 400;
            res.set_content("bad path", "text/plain; charset=utf-8");
            return;
        }

        std::filesystem::path p = std::filesystem::path("swagger-ui") / rel;
        std::string body = read_file_or_empty(p);
        if (body.empty()) {
            res.status = 404;
            res.set_content("not found", "text/plain; charset=utf-8");
            return;
        }

        res.set_content(body, guess_mime(p.string()));
    });

    // IMPORTANT: Swagger UI по умолчанию смотрит на petstore.
    // Нужно поправить swagger-ui/swagger-initializer.js:
    // url: "/openapi.json"

    const char* host = "0.0.0.0";
    int port = 8080;
    std::cout << "listening on http://" << host << ":" << port << std::endl;
    if (!svr.listen(host, port)) {
        std::cerr << "failed to listen on " << host << ":" << port << std::endl;
        return 1;
    }
    return 0;
}
