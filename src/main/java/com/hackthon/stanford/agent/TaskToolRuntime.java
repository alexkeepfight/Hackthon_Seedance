package com.hackthon.stanford.agent;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Tool execution runtime aligned with {@code TaskSystemV5StreamAgent.TaskToolSupport} (without Spring / ChatBI).
 */
@Slf4j
public final class TaskToolRuntime {

    private static final int MAX_OUTPUT_LENGTH = 50_000;
    private static final DateTimeFormatter SESSION_DIR_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final ConcurrentHashMap<String, Path> SESSION_DIR_CACHE = new ConcurrentHashMap<>();

    private TaskToolRuntime() {
    }

    public static JSONObject executeTool(String sessionId, String userId, String toolName, JSONObject args) {
        if (toolName == null || toolName.isBlank()) {
            return error("missing tool name");
        }
        try {
            log.info("[TaskTool] userId={}, sessionId={}, tool={}, args={}",
                    sanitizeUserId(userId), normalizedSessionId(sessionId), toolName,
                    args == null ? "{}" : args.toJSONString());
            return switch (toolName) {
                case "bash" -> executeBash(args);
                case "read_file" -> executeReadFile(args);
                case "write_file" -> executeWriteFile(args);
                case "edit_file" -> executeEditFile(args);
                case "task_create" -> executeCreate(userId, sessionId, args);
                case "task_update" -> executeUpdate(userId, sessionId, args);
                case "task_list" -> executeList(userId, sessionId);
                case "task_get" -> executeGet(userId, sessionId, args);
                case "chatbi_master_pipeline" -> executeChatBiStub(args);
                default -> error("unknown tool: " + toolName);
            };
        } catch (Exception e) {
            return error(e.getMessage() == null ? "tool failed" : e.getMessage());
        }
    }

    private static JSONObject executeBash(JSONObject args) {
        String command = args == null ? null : args.getString("command");
        if (command == null || command.isBlank()) {
            return error("missing command");
        }
        return textResult(runBash(command));
    }

    private static JSONObject executeReadFile(JSONObject args) {
        String path = args == null ? null : args.getString("path");
        Integer limit = args == null ? null : args.getInteger("limit");
        if (path == null || path.isBlank()) {
            return error("missing path");
        }
        return textResult(runRead(path, limit));
    }

    private static JSONObject executeWriteFile(JSONObject args) {
        String path = args == null ? null : args.getString("path");
        String content = args == null ? null : args.getString("content");
        if (path == null || path.isBlank()) {
            return error("missing path");
        }
        if (content == null) {
            return error("missing content");
        }
        return textResult(runWrite(path, content));
    }

    private static JSONObject executeEditFile(JSONObject args) {
        String path = args == null ? null : args.getString("path");
        String oldText = args == null ? null : args.getString("old_text");
        String newText = args == null ? null : args.getString("new_text");
        if (path == null || path.isBlank()) {
            return error("missing path");
        }
        if (oldText == null) {
            return error("missing old_text");
        }
        if (newText == null) {
            return error("missing new_text");
        }
        return textResult(runEdit(path, oldText, newText));
    }

    private static JSONObject executeCreate(String userId, String sessionId, JSONObject args) {
        String subject = args == null ? null : args.getString("subject");
        String description = args == null ? "" : args.getString("description");
        if (subject == null || subject.isBlank()) {
            return error("missing subject");
        }
        return textResult(taskManager(userId, sessionId).create(subject.trim(), description == null ? "" : description.trim()));
    }

    private static JSONObject executeUpdate(String userId, String sessionId, JSONObject args) {
        if (args == null || args.getInteger("task_id") == null) {
            return error("missing task_id");
        }
        int taskId = args.getIntValue("task_id");
        String status = args.getString("status");
        List<Integer> addBlockedBy = toIntegerList(args.getJSONArray("addBlockedBy"));
        List<Integer> removeBlockedBy = toIntegerList(args.getJSONArray("removeBlockedBy"));
        return textResult(taskManager(userId, sessionId).update(taskId, status, addBlockedBy, removeBlockedBy));
    }

    private static JSONObject executeList(String userId, String sessionId) {
        return textResult(taskManager(userId, sessionId).listAll());
    }

    private static JSONObject executeGet(String userId, String sessionId, JSONObject args) {
        if (args == null || args.getInteger("task_id") == null) {
            return error("missing task_id");
        }
        return textResult(taskManager(userId, sessionId).get(args.getIntValue("task_id")));
    }

    private static JSONObject executeChatBiStub(JSONObject args) {
        String query = args == null ? null : args.getString("query");
        if (query == null || query.isBlank()) {
            return error("missing query");
        }
        String msg = "[chatbi_master_pipeline stub] Not connected to DeepChatBI MasterV5Agent. "
                + "Your query was: " + query.trim();
        return textResult(msg);
    }

    private static List<Integer> toIntegerList(JSONArray array) {
        List<Integer> values = new ArrayList<>();
        if (array == null) {
            return values;
        }
        for (int i = 0; i < array.size(); i++) {
            Integer v = array.getInteger(i);
            if (v != null) {
                values.add(v);
            }
        }
        return values;
    }

    private static JSONObject error(String message) {
        JSONObject out = new JSONObject(new LinkedHashMap<>());
        out.put("error", message);
        return out;
    }

    private static JSONObject textResult(String content) {
        JSONObject out = new JSONObject(new LinkedHashMap<>());
        out.put("content", content);
        return out;
    }

    private static TaskManager taskManager(String userId, String sessionId) {
        Path dir = sessionTasksDir(userId, sessionId);
        return new TaskManager(dir);
    }

    static Path sessionTasksDir(String userId, String sessionId) {
        String safeUser = sanitizeUserId(userId);
        String safeSession = normalizedSessionId(sessionId);
        String cacheKey = safeUser + "/" + safeSession;
        return SESSION_DIR_CACHE.computeIfAbsent(cacheKey, k -> {
            Path sessionsRoot = workspaceRoot()
                    .resolve("logs")
                    .resolve(safeUser)
                    .resolve("sessions");
            try {
                Files.createDirectories(sessionsRoot);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create sessions root: " + sessionsRoot, e);
            }
            String suffix = "_" + safeSession;
            try (Stream<Path> dirs = Files.list(sessionsRoot)) {
                Path existing = dirs
                        .filter(Files::isDirectory)
                        .filter(p -> p.getFileName().toString().endsWith(suffix))
                        .max(Comparator.comparing(p -> p.getFileName().toString()))
                        .orElse(null);
                if (existing != null) {
                    return existing;
                }
            } catch (IOException e) {
                log.warn("Failed to scan sessions dir: {}", sessionsRoot, e);
            }
            String timestamp = LocalDateTime.now().format(SESSION_DIR_FMT);
            Path newDir = sessionsRoot.resolve(timestamp + suffix);
            try {
                Files.createDirectories(newDir);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create session dir: " + newDir, e);
            }
            log.info("[TaskTool] created session dir: {}", newDir);
            return newDir;
        });
    }

    static String sanitizeUserId(String userId) {
        if (userId == null || userId.isBlank()) {
            return "anonymous";
        }
        String cleaned = userId.replaceAll("[^a-zA-Z]", "");
        return cleaned.isBlank() ? "anonymous" : cleaned;
    }

    static String normalizedSessionId(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return "default-session";
        }
        return sessionId.trim().replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private static Path workspaceRoot() {
        return Path.of(System.getProperty("user.dir")).normalize().toAbsolutePath();
    }

    private static String runBash(String command) {
        List<String> dangerous = List.of("rm -rf /", "sudo", "shutdown", "reboot", "> /dev/");
        for (String token : dangerous) {
            if (command.contains(token)) {
                return "Error: Dangerous command blocked";
            }
        }
        try {
            Process process = new ProcessBuilder("bash", "-lc", command)
                    .directory(workspaceRoot().toFile())
                    .redirectErrorStream(true)
                    .start();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            Thread reader = new Thread(() -> copy(process.getInputStream(), output));
            reader.setDaemon(true);
            reader.start();
            boolean finished = process.waitFor(120, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                return "Error: Timeout (120s)";
            }
            reader.join(1000);
            String out = truncate(output.toString(StandardCharsets.UTF_8), MAX_OUTPUT_LENGTH).trim();
            return out.isEmpty() ? "(no output)" : out;
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    private static String runRead(String path, Integer limit) {
        try {
            List<String> lines = Files.readAllLines(safePath(path), StandardCharsets.UTF_8);
            if (limit != null && limit > 0 && limit < lines.size()) {
                int more = lines.size() - limit;
                lines = new ArrayList<>(lines.subList(0, limit));
                lines.add("... (" + more + " more)");
            }
            return truncate(String.join("\n", lines), MAX_OUTPUT_LENGTH);
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    private static String runWrite(String path, String content) {
        try {
            Path filePath = safePath(path);
            Path parent = filePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(filePath, content, StandardCharsets.UTF_8);
            return "Wrote " + content.length() + " bytes";
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    private static String runEdit(String path, String oldText, String newText) {
        try {
            Path filePath = safePath(path);
            String content = Files.readString(filePath, StandardCharsets.UTF_8);
            int index = content.indexOf(oldText);
            if (index < 0) {
                return "Error: Text not found in " + path;
            }
            String updated = content.substring(0, index) + newText + content.substring(index + oldText.length());
            Files.writeString(filePath, updated, StandardCharsets.UTF_8);
            return "Edited " + path;
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    private static Path safePath(String path) {
        Path resolved = workspaceRoot().resolve(path).normalize().toAbsolutePath();
        if (!resolved.startsWith(workspaceRoot())) {
            throw new IllegalArgumentException("Path escapes workspace: " + path);
        }
        return resolved;
    }

    private static String truncate(String text, int maxLength) {
        if (text == null) {
            return "";
        }
        return text.length() <= maxLength ? text : text.substring(0, maxLength);
    }

    private static void copy(InputStream in, ByteArrayOutputStream out) {
        byte[] buffer = new byte[4096];
        int read;
        try {
            while ((read = in.read(buffer)) >= 0) {
                out.write(buffer, 0, read);
            }
        } catch (IOException ignore) {
        }
    }

    private enum TaskStatus {
        pending, in_progress, completed;

        static List<String> names() {
            return List.of(pending.name(), in_progress.name(), completed.name());
        }
    }

    @Data
    @NoArgsConstructor
    static class TaskRecord {
        private Integer id;
        private String subject;
        private String description;
        private String status;
        private List<Integer> blockedBy = new ArrayList<>();
        private String owner;
    }

    private static final class TaskManager {
        private final Path tasksDir;
        private int nextId;

        private TaskManager(Path tasksDir) {
            this.tasksDir = tasksDir;
            ensureDir();
            this.nextId = maxId() + 1;
        }

        private synchronized String create(String subject, String description) {
            TaskRecord task = new TaskRecord();
            task.setId(nextId);
            task.setSubject(subject);
            task.setDescription(description == null ? "" : description);
            task.setStatus(TaskStatus.pending.name());
            task.setBlockedBy(new ArrayList<>());
            task.setOwner("");
            save(task);
            nextId++;
            return toJson(task);
        }

        private synchronized String get(int taskId) {
            return toJson(load(taskId));
        }

        private synchronized String update(int taskId, String status, List<Integer> addBlockedBy, List<Integer> removeBlockedBy) {
            TaskRecord task = load(taskId);
            if (status != null && !status.isBlank()) {
                String normalized = status.trim().toLowerCase(Locale.ROOT);
                if (!TaskStatus.names().contains(normalized)) {
                    throw new IllegalArgumentException("Invalid status: " + status);
                }
                task.setStatus(normalized);
                if (TaskStatus.completed.name().equals(normalized)) {
                    clearDependency(taskId);
                }
            }
            List<Integer> blockedBy = task.getBlockedBy() == null ? new ArrayList<>() : new ArrayList<>(task.getBlockedBy());
            if (addBlockedBy != null) {
                for (Integer id : addBlockedBy) {
                    if (id != null && !Objects.equals(id, taskId) && !blockedBy.contains(id)) {
                        blockedBy.add(id);
                    }
                }
            }
            if (removeBlockedBy != null && !removeBlockedBy.isEmpty()) {
                blockedBy.removeIf(removeBlockedBy::contains);
            }
            blockedBy.sort(Comparator.naturalOrder());
            task.setBlockedBy(blockedBy);
            save(task);
            return toJson(task);
        }

        private synchronized String listAll() {
            List<TaskRecord> tasks = loadAll();
            if (tasks.isEmpty()) {
                return "No tasks.";
            }
            List<String> lines = new ArrayList<>();
            for (TaskRecord task : tasks) {
                String blocked = task.getBlockedBy() == null || task.getBlockedBy().isEmpty()
                        ? ""
                        : " (blocked by: " + task.getBlockedBy() + ")";
                lines.add(marker(task.getStatus()) + " #" + task.getId() + ": " + task.getSubject() + blocked);
            }
            return String.join("\n", lines);
        }

        private String marker(String status) {
            return switch (status) {
                case "pending" -> "[ ]";
                case "in_progress" -> "[>]";
                case "completed" -> "[x]";
                default -> "[?]";
            };
        }

        private void clearDependency(int completedId) {
            for (TaskRecord task : loadAll()) {
                List<Integer> blockedBy = task.getBlockedBy();
                if (blockedBy != null && blockedBy.removeIf(id -> Objects.equals(id, completedId))) {
                    save(task);
                }
            }
        }

        private TaskRecord load(int taskId) {
            Path path = taskPath(taskId);
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Task " + taskId + " not found");
            }
            return loadFromPath(path);
        }

        private TaskRecord loadFromPath(Path path) {
            try {
                String json = Files.readString(path, StandardCharsets.UTF_8);
                TaskRecord task = JSONObject.parseObject(json, TaskRecord.class);
                if (task == null) {
                    throw new IllegalStateException("Invalid task json: " + path);
                }
                if (task.getBlockedBy() == null) {
                    task.setBlockedBy(new ArrayList<>());
                }
                return task;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load task: " + path, e);
            }
        }

        private void save(TaskRecord task) {
            Path path = taskPath(task.getId());
            try {
                Files.writeString(path, toJson(task), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to save task: " + task.getId(), e);
            }
        }

        private List<TaskRecord> loadAll() {
            ensureDir();
            try (Stream<Path> stream = Files.list(tasksDir)) {
                return stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().startsWith("task_"))
                        .filter(p -> p.getFileName().toString().endsWith(".json"))
                        .sorted(Comparator.comparingInt(this::extractId))
                        .map(this::loadFromPath)
                        .toList();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to list tasks", e);
            }
        }

        private int maxId() {
            ensureDir();
            try (Stream<Path> stream = Files.list(tasksDir)) {
                return stream
                        .filter(Files::isRegularFile)
                        .mapToInt(this::extractId)
                        .max()
                        .orElse(0);
            } catch (IOException e) {
                return 0;
            }
        }

        private int extractId(Path path) {
            String fileName = path.getFileName().toString();
            if (!fileName.startsWith("task_") || !fileName.endsWith(".json")) {
                return -1;
            }
            try {
                return Integer.parseInt(fileName.substring(5, fileName.length() - 5));
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        private Path taskPath(int taskId) {
            return tasksDir.resolve("task_" + taskId + ".json");
        }

        private String toJson(TaskRecord task) {
            return JSON.toJSONString(task, JSONWriter.Feature.PrettyFormat);
        }

        private void ensureDir() {
            try {
                Files.createDirectories(tasksDir);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create tasks dir: " + tasksDir, e);
            }
        }
    }
}
