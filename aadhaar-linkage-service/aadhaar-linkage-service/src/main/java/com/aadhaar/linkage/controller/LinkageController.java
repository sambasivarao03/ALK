package com.aadhaar.linkage.controller;

import com.aadhaar.linkage.dto.LinkageRequest;
import com.aadhaar.linkage.dto.LinkageResponse;
import com.aadhaar.linkage.service.LinkageService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Single endpoint controller. Uses explicit constructor injection to avoid Lombok issues.
 */
@RestController
@RequestMapping("/api/v1/person")
public class LinkageController {

    private final LinkageService linkageService;

    public LinkageController(LinkageService linkageService) {
        this.linkageService = linkageService;
    }

    @PostMapping("/manage")
    public ResponseEntity<LinkageResponse> processRequest(@Valid @RequestBody LinkageRequest request) {
        LinkageResponse response = linkageService.processRequest(request);
        return ResponseEntity.ok(response);
    }

}


