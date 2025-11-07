package com.aadhaar.linkage.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "person_identity")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PersonIdentity {

    @Id
    @Column(name = "aadhaar_linkage_key", nullable = false, updatable = false, unique = true)
    private String aadhaarLinkageKey;

    // Hashed Personal Identifiers
    @Column(name = "hashed_aadhaar_number", length = 64)
    private String hashedAadhaarNumber;

    @Column(name = "hashed_pan_number", length = 64)
    private String hashedPanNumber;

    @Column(name = "hashed_voter_id", length = 64)
    private String hashedVoterId;

    @Column(name = "hashed_dl_number", length = 64)
    private String hashedDlNumber;

    // Hashed Personal Info
    @Column(name = "hashed_forename", length = 64)
    private String hashedForename;

    @Column(name = "hashed_secondname", length = 64)
    private String hashedSecondname;

    @Column(name = "hashed_lastname", length = 64)
    private String hashedLastname;

    @Column(name = "hashed_dob", length = 64)
    private String hashedDob;

    @Column(name = "hashed_address", length = 128)
    private String hashedAddress;

    @Column(name = "gender", length = 10)
    private String gender;

    // Counters
    @Column(name = "aadhaar_counter")
    private int aadhaarCounter;

    @Column(name = "pan_counter")
    private int panCounter;

    @Column(name = "voter_id_counter")
    private int voterIdCounter;

    @Column(name = "dl_counter")
    private int dlCounter;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    public void prePersist() {
        if (this.aadhaarLinkageKey == null) {
            this.aadhaarLinkageKey = UUID.randomUUID().toString();
        }
    }

	public String getAadhaarLinkageKey() {
		return aadhaarLinkageKey;
	}

	public void setAadhaarLinkageKey(String aadhaarLinkageKey) {
		this.aadhaarLinkageKey = aadhaarLinkageKey;
	}

	public String getHashedAadhaarNumber() {
		return hashedAadhaarNumber;
	}

	public void setHashedAadhaarNumber(String hashedAadhaarNumber) {
		this.hashedAadhaarNumber = hashedAadhaarNumber;
	}

	public String getHashedPanNumber() {
		return hashedPanNumber;
	}

	public void setHashedPanNumber(String hashedPanNumber) {
		this.hashedPanNumber = hashedPanNumber;
	}

	public String getHashedVoterId() {
		return hashedVoterId;
	}

	public void setHashedVoterId(String hashedVoterId) {
		this.hashedVoterId = hashedVoterId;
	}

	public String getHashedDlNumber() {
		return hashedDlNumber;
	}

	public void setHashedDlNumber(String hashedDlNumber) {
		this.hashedDlNumber = hashedDlNumber;
	}

	public String getHashedForename() {
		return hashedForename;
	}

	public void setHashedForename(String hashedForename) {
		this.hashedForename = hashedForename;
	}

	public String getHashedSecondname() {
		return hashedSecondname;
	}

	public void setHashedSecondname(String hashedSecondname) {
		this.hashedSecondname = hashedSecondname;
	}

	public String getHashedLastname() {
		return hashedLastname;
	}

	public void setHashedLastname(String hashedLastname) {
		this.hashedLastname = hashedLastname;
	}

	public String getHashedDob() {
		return hashedDob;
	}

	public void setHashedDob(String hashedDob) {
		this.hashedDob = hashedDob;
	}

	public String getHashedAddress() {
		return hashedAddress;
	}

	public void setHashedAddress(String hashedAddress) {
		this.hashedAddress = hashedAddress;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public int getAadhaarCounter() {
		return aadhaarCounter;
	}

	public void setAadhaarCounter(int aadhaarCounter) {
		this.aadhaarCounter = aadhaarCounter;
	}

	public int getPanCounter() {
		return panCounter;
	}

	public void setPanCounter(int panCounter) {
		this.panCounter = panCounter;
	}

	public int getVoterIdCounter() {
		return voterIdCounter;
	}

	public void setVoterIdCounter(int voterIdCounter) {
		this.voterIdCounter = voterIdCounter;
	}

	public int getDlCounter() {
		return dlCounter;
	}

	public void setDlCounter(int dlCounter) {
		this.dlCounter = dlCounter;
	}

	public LocalDateTime getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(LocalDateTime createdAt) {
		this.createdAt = createdAt;
	}

	public LocalDateTime getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(LocalDateTime updatedAt) {
		this.updatedAt = updatedAt;
	}
    
}
