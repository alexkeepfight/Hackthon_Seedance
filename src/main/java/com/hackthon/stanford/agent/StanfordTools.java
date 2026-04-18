package com.hackthon.stanford.agent;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Claude Messages API tool definitions (same capabilities as TaskSystemV5StreamAgent: bash, files, tasks, ChatBI stub).
 */
public final class StanfordTools {

    private StanfordTools() {
    }

    public static JSONArray claudeTools() {
        JSONArray arr = new JSONArray();
        arr.add(tool("bash",
                "Run a shell command in the workspace root. Session logs: logs/<user>/sessions/",
                mergeProps(req("command", "string", "Shell command")),
                List.of("command")));
        arr.add(tool("read_file", "Read file contents under workspace.",
                mergeProps(
                        req("path", "string", "Relative path from workspace root"),
                        opt("limit", "integer", "Max lines")
                ),
                List.of("path")));
        arr.add(tool("write_file", "Write content to a file under workspace.",
                mergeProps(
                        req("path", "string", "Relative path"),
                        req("content", "string", "File content")
                ),
                List.of("path", "content")));
        arr.add(tool("edit_file", "Replace exact text in a file.",
                mergeProps(
                        req("path", "string", "Relative path"),
                        req("old_text", "string", "Text to find"),
                        req("new_text", "string", "Replacement")
                ),
                List.of("path", "old_text", "new_text")));
        arr.add(tool("task_create", "Create a task on the session task board.",
                mergeProps(
                        req("subject", "string", "Short title"),
                        opt("description", "string", "Details")
                ),
                List.of("subject")));
        arr.add(tool("task_update", "Update task status or dependencies.",
                mergeProps(
                        req("task_id", "integer", "Task id"),
                        optEnum("status", List.of("pending", "in_progress", "completed")),
                        optArr("addBlockedBy", "integer"),
                        optArr("removeBlockedBy", "integer")
                ),
                List.of("task_id")));
        arr.add(tool("task_list", "List all tasks.", mergeProps(), List.of()));
        arr.add(tool("task_get", "Get one task by id.",
                mergeProps(req("task_id", "integer", "Task id")),
                List.of("task_id")));
        arr.add(tool("chatbi_master_pipeline",
                "Stub: in DeepChatBI this runs PlanV5 → task_chain. Here returns a placeholder string.",
                mergeProps(req("query", "string", "Natural language question")),
                List.of("query")));
        return arr;
    }

    private static JSONObject tool(String name, String description, JSONObject properties, List<String> required) {
        JSONObject inputSchema = new JSONObject();
        inputSchema.put("type", "object");
        inputSchema.put("properties", properties);
        inputSchema.put("required", required);
        JSONObject o = new JSONObject();
        o.put("name", name);
        o.put("description", description);
        o.put("input_schema", inputSchema);
        return o;
    }

    private static JSONObject mergeProps(JSONObject... entries) {
        JSONObject o = new JSONObject(new LinkedHashMap<>());
        if (entries == null) {
            return o;
        }
        for (JSONObject jo : entries) {
            if (jo == null) {
                continue;
            }
            for (String k : jo.keySet()) {
                o.put(k, jo.get(k));
            }
        }
        return o;
    }

    private static JSONObject req(String name, String type, String desc) {
        JSONObject p = new JSONObject();
        p.put("type", type);
        p.put("description", desc);
        return new JSONObject(Map.of(name, p));
    }

    private static JSONObject opt(String name, String type, String desc) {
        return req(name, type, desc);
    }

    private static JSONObject optEnum(String name, List<String> values) {
        JSONObject p = new JSONObject();
        p.put("type", "string");
        p.put("enum", values);
        return new JSONObject(Map.of(name, p));
    }

    private static JSONObject optArr(String name, String itemType) {
        JSONObject items = new JSONObject();
        items.put("type", itemType);
        JSONObject p = new JSONObject();
        p.put("type", "array");
        p.put("items", items);
        return new JSONObject(Map.of(name, p));
    }
}
