package com.aadhaar.linkage.service;

import com.aadhaar.linkage.dto.LinkageRequest;
import com.aadhaar.linkage.dto.LinkageResponse;
import com.aadhaar.linkage.model.PersonIdentity;
import com.aadhaar.linkage.repository.LinkageRepository;
import com.aadhaar.linkage.util.HashUtil;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Optional;

/**
 * Service implements the core actions: INSERT, UPDATE, DELETE, SEARCH.
 * Uses explicit constructor injection (no Lombok) to avoid "blank final" problems.
 */
@Service
@Transactional
public class LinkageService {

    private final LinkageRepository linkageRepository;

    public LinkageService(LinkageRepository linkageRepository) {
        this.linkageRepository = linkageRepository;
    }

    public LinkageResponse processRequest(LinkageRequest request) {
        if (request == null || request.getAction() == null) {
            return LinkageResponse.error("Invalid request");
        }
        String action = request.getAction().trim().toUpperCase();
        switch (action) {
            case "INSERT": return insertRecord(request);
            case "UPDATE": return updateRecord(request);
            case "DELETE": return deleteRecord(request);
            case "SEARCH": return searchRecord(request);
            default: return LinkageResponse.error("Invalid action type: " + request.getAction());
        }
    }

    private LinkageResponse insertRecord(LinkageRequest request) {
        Map<String, String> data = request.getData();
        if (data == null) return LinkageResponse.error("Missing data");

        PersonIdentity person = new PersonIdentity();
        // ALK will be generated in @PrePersist of entity if not set
        person.setHashedAadhaarNumber(hash(data.get("aadhaar_number")));
        person.setHashedPanNumber(hash(data.get("pan_number")));
        person.setHashedVoterId(hash(data.get("voter_id")));
        person.setHashedDlNumber(hash(data.get("dl_number")));
        person.setHashedForename(hash(data.get("forename")));
        person.setHashedSecondname(hash(data.get("secondname")));
        person.setHashedLastname(hash(data.get("lastname")));
        person.setHashedDob(hash(data.get("dob")));
        person.setHashedAddress(hash(data.get("address")));
        person.setGender(data.get("gender"));

        // initialize counters as needed (source-based in production)
        linkageRepository.save(person);

        return LinkageResponse.success("Record inserted successfully", person);
    }

    private LinkageResponse updateRecord(LinkageRequest request) {
        String oldKey = request.getOldAadhaarLinkageKey();
        if (oldKey == null) return LinkageResponse.error("oldAadhaarLinkageKey required");
        Optional<PersonIdentity> opt = linkageRepository.findById(oldKey);
        if (opt.isEmpty()) return LinkageResponse.error("Record not found");
        PersonIdentity person = opt.get();
        Map<String, String> data = request.getData();
        if (data != null) {
            if (data.containsKey("aadhaar_number")) person.setHashedAadhaarNumber(hash(data.get("aadhaar_number")));
            if (data.containsKey("pan_number")) person.setHashedPanNumber(hash(data.get("pan_number")));
            if (data.containsKey("voter_id")) person.setHashedVoterId(hash(data.get("voter_id")));
            if (data.containsKey("dl_number")) person.setHashedDlNumber(hash(data.get("dl_number")));
            if (data.containsKey("forename")) person.setHashedForename(hash(data.get("forename")));
            if (data.containsKey("secondname")) person.setHashedSecondname(hash(data.get("secondname")));
            if (data.containsKey("lastname")) person.setHashedLastname(hash(data.get("lastname")));
            if (data.containsKey("dob")) person.setHashedDob(hash(data.get("dob")));
            if (data.containsKey("address")) person.setHashedAddress(hash(data.get("address")));
            if (data.containsKey("gender")) person.setGender(data.get("gender"));
        }
        linkageRepository.save(person);
        return LinkageResponse.success("Record updated successfully", person);
    }

    private LinkageResponse deleteRecord(LinkageRequest request) {
        String oldKey = request.getOldAadhaarLinkageKey();
        if (oldKey == null) return LinkageResponse.error("oldAadhaarLinkageKey required");
        Optional<PersonIdentity> opt = linkageRepository.findById(oldKey);
        if (opt.isEmpty()) return LinkageResponse.error("Record not found");
        linkageRepository.delete(opt.get());
        return LinkageResponse.success("Record deleted successfully");
    }

    private LinkageResponse searchRecord(LinkageRequest request) {
        Map<String, String> data = request.getData();
        if (data == null) return LinkageResponse.error("Missing data for search");
        String hAadhaar = hash(data.get("aadhaar_number"));
        String hDob = hash(data.get("dob"));
        String hForename = hash(data.get("forename"));
        String hLastname = hash(data.get("lastname"));

        Optional<PersonIdentity> personOpt =
                linkageRepository.findByHashedAadhaarNumberAndHashedDobAndHashedForenameAndHashedLastname(
                        hAadhaar, hDob, hForename, hLastname);

        return personOpt
                .map(p -> LinkageResponse.success("Record found", p))
                .orElseGet(() -> new LinkageResponse("NOT_FOUND", "No record found", null));
    }

    private static String hash(String s) {
        return s == null ? null : HashUtil.sha256(s);
    }
}
