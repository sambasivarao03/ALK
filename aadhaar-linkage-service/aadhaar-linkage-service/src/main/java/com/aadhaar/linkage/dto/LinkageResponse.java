package com.aadhaar.linkage.dto;

/**
 * Simple POJO used as API response. Plain Java (no Lombok) so builder() issues don't block compilation.
 */
public class LinkageResponse {
    private String status;
    private String message;
    private Object data;

    public LinkageResponse() {}

    public LinkageResponse(String status, String message, Object data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public static LinkageResponse success(String message, Object data) {
        return new LinkageResponse("SUCCESS", message, data);
    }
    public static LinkageResponse success(String message) {
        return new LinkageResponse("SUCCESS", message, null);
    }
    public static LinkageResponse error(String message) {
        return new LinkageResponse("ERROR", message, null);
    }

    // getters / setters
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Object getData() { return data; }
    public void setData(Object data) { this.data = data; }
}
