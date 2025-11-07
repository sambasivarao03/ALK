package com.aadhaar.linkage.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.*;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class LinkageRequest {

    @NotBlank(message = "Action must be provided")
    private String action; // INSERT | UPDATE | DELETE | SEARCH

    @NotBlank(message = "Source must be provided")
    private String source; // Aadhaar | PAN | Voter | Driving

    // Generic container for person data
    private Map<String, String> data;

    // Used in update/delete/search
    private String oldAadhaarLinkageKey;

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public Map<String, String> getData() {
		return data;
	}

	public void setData(Map<String, String> data) {
		this.data = data;
	}

	public String getOldAadhaarLinkageKey() {
		return oldAadhaarLinkageKey;
	}

	public void setOldAadhaarLinkageKey(String oldAadhaarLinkageKey) {
		this.oldAadhaarLinkageKey = oldAadhaarLinkageKey;
	}
    
    
}
