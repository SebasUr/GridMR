package com.gridmr.master.http;

import com.gridmr.master.service.ControlServiceImpl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HttpJobServer {
    private final ControlServiceImpl control;
    private final int port;
    private HttpServer server;

    public HttpJobServer(ControlServiceImpl control, int port) {
        this.control = control;
        this.port = port;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/submit-job", new SubmitJobHandler(control));
        server.createContext("/health", exchange -> {
            byte[] ok = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, ok.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(ok); }
        });
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        System.out.println("HTTP job server listening on port " + port);
    }

    public void stop() {
        if (server != null) server.stop(0);
    }

    static class SubmitJobHandler implements HttpHandler {
        private final ControlServiceImpl control;
        SubmitJobHandler(ControlServiceImpl control) { this.control = control; }

        @Override public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                respond(exchange, 405, jsonError("method not allowed"));
                return;
            }
            String ctype = Optional.ofNullable(exchange.getRequestHeaders().getFirst("Content-Type"))
                    .orElse("").toLowerCase(Locale.ROOT);
            Map<String, String> form = new HashMap<>();
            String body = readBody(exchange.getRequestBody());
            if (ctype.contains("application/x-www-form-urlencoded")) {
                form.putAll(parseFormUrlencoded(body));
            } else if (ctype.contains("application/json")) {
                // Very naive JSON parser for flat string/number/bool fields
                form.putAll(parseNaiveJson(body));
            } else {
                // Try to parse as key=value lines or fallback to query-like
                form.putAll(parseKeyValueLines(body));
            }

            try {
                List<String> inputs = splitCsv(form.getOrDefault("input_uris", ""));
                Integer nReducers = parseInt(form.get("n_reducers"));
                String mapBin = form.getOrDefault("map_bin_uri", "");
                String reduceBin = form.getOrDefault("reduce_bin_uri", "");
                Integer desiredMaps = parseInt(form.get("desired_maps"));
                Boolean groupPart = parseBool(form.get("group_partitioning"));
                Integer minWorkers = parseInt(form.get("min_workers"));
                Long startDelayMs = parseLong(form.get("start_delay_ms"));

                String jobId = control.submitJob(inputs, nReducers, mapBin, reduceBin, desiredMaps, groupPart, minWorkers, startDelayMs);
                respond(exchange, 200, "{\"job_id\":\"" + jobId + "\",\"status\":\"accepted\"}");
            } catch (Exception e) {
                respond(exchange, 400, jsonError(e.getMessage()));
            }
        }

        private static String readBody(InputStream is) throws IOException {
            byte[] buf = is.readAllBytes();
            return new String(buf, StandardCharsets.UTF_8);
        }

        private static Map<String,String> parseFormUrlencoded(String s) {
            Map<String,String> m = new HashMap<>();
            for (String pair : s.split("&")) {
                int eq = pair.indexOf('=');
                if (eq < 0) continue;
                String k = urlDecode(pair.substring(0, eq));
                String v = urlDecode(pair.substring(eq+1));
                m.put(k, v);
            }
            return m;
        }

        private static Map<String,String> parseNaiveJson(String s) {
            Map<String,String> m = new HashMap<>();
            String t = s.trim();
            if (t.startsWith("{") && t.endsWith("}")) t = t.substring(1, t.length()-1);
            // Split into top-level key:value pairs by commas not inside quotes
            List<String> parts = new ArrayList<>();
            StringBuilder cur = new StringBuilder();
            boolean inStr = false; char q = '\0';
            for (int i = 0; i < t.length(); i++) {
                char ch = t.charAt(i);
                if (inStr) {
                    if (ch == q && (i == 0 || t.charAt(i-1) != '\\')) inStr = false;
                    cur.append(ch);
                } else {
                    if (ch == '"' || ch == '\'') { inStr = true; q = ch; cur.append(ch); }
                    else if (ch == ',') { parts.add(cur.toString()); cur.setLength(0); }
                    else cur.append(ch);
                }
            }
            if (cur.length() > 0) parts.add(cur.toString());
            for (String part : parts) {
                String pp = part;
                // split on first colon not inside quotes
                int cpos = -1; inStr = false; q='\0';
                for (int i = 0; i < pp.length(); i++) {
                    char ch = pp.charAt(i);
                    if (inStr) {
                        if (ch == q && (i == 0 || pp.charAt(i-1) != '\\')) inStr = false;
                    } else {
                        if (ch == '"' || ch == '\'') { inStr = true; q = ch; }
                        else if (ch == ':') { cpos = i; break; }
                    }
                }
                if (cpos < 0) continue;
                String k = stripQuotes(pp.substring(0,cpos).trim());
                String v = stripQuotes(pp.substring(cpos+1).trim());
                m.put(k, v);
            }
            return m;
        }

        private static Map<String,String> parseKeyValueLines(String s) {
            Map<String,String> m = new HashMap<>();
            for (String line : s.split("\n")) {
                line = line.trim();
                if (line.isEmpty()) continue;
                int eq = line.indexOf('=');
                if (eq < 0) continue;
                m.put(line.substring(0,eq).trim(), line.substring(eq+1).trim());
            }
            return m;
        }

        private static String stripQuotes(String s){
            s = s.trim();
            if (s.startsWith("\"") && s.endsWith("\"")) return s.substring(1, s.length()-1);
            if (s.startsWith("'") && s.endsWith("'")) return s.substring(1, s.length()-1);
            return s;
        }

        private static String urlDecode(String s){
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        }

        private static List<String> splitCsv(String csv){
            List<String> out = new ArrayList<>();
            for (String p : csv.split(",")) {
                String t = p.trim();
                if (!t.isEmpty()) out.add(t);
            }
            return out;
        }

        private static Integer parseInt(String s){
            try { return (s==null||s.isEmpty())? null : Integer.parseInt(s); } catch(Exception e){ return null; }
        }
        private static Long parseLong(String s){
            try { return (s==null||s.isEmpty())? null : Long.parseLong(s); } catch(Exception e){ return null; }
        }
        private static Boolean parseBool(String s){
            if (s == null) return null;
            String t = s.trim().toLowerCase(Locale.ROOT);
            if (t.equals("1") || t.equals("true") || t.equals("yes")) return true;
            if (t.equals("0") || t.equals("false") || t.equals("no")) return false;
            return null;
        }

        private static void respond(HttpExchange ex, int code, String body) throws IOException {
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            ex.sendResponseHeaders(code, b.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(b); }
        }

        private static String jsonError(String msg){
            String safe = msg == null ? "" : msg.replace("\"", "'");
            return "{\"error\":\"" + safe + "\"}";
        }
    }
}
