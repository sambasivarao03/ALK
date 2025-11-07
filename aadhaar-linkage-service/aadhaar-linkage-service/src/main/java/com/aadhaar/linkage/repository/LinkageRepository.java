package com.aadhaar.linkage.repository;

import com.aadhaar.linkage.model.PersonIdentity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface LinkageRepository extends JpaRepository<PersonIdentity, String> {

    Optional<PersonIdentity> findByHashedAadhaarNumberAndHashedDobAndHashedForenameAndHashedLastname(
            String hashedAadhaarNumber,
            String hashedDob,
            String hashedForename,
            String hashedLastname
    );
}
