# ALK
Aadhar-Linkage-MicroService

# 🪪 Aadhaar Linkage Microservice

A Spring Boot–based backend service for securely linking and managing identity records (Aadhaar, PAN, DL, etc.) using SHA-256 hashing.  
All operations (INSERT, UPDATE, DELETE, SEARCH) are handled through a **single dynamic endpoint**.

---

## 🚀 Features

- ✅ Single unified API endpoint: `/api/v1/person/manage`
- 🔒 SHA-256 hashing for all sensitive fields (Aadhaar, PAN, etc.)
- 🧩 Supports CRUD operations via an `action` field (`INSERT`, `UPDATE`, `DELETE`, `SEARCH`)
- 📘 Integrated Swagger UI for easy testing
- 🗄️ JPA + H2 / PostgreSQL ready persistence
- 🧠 Explicit constructor injection (no Lombok dependency issues)
- 🧰 Fully transactional service layer

---

## 🧱 Project Structure

```
com.aadhaar.linkage
├── controller/
│   └── LinkageController.java
├── dto/
│   ├── LinkageRequest.java
│   └── LinkageResponse.java
├── model/
│   └── PersonIdentity.java
├── repository/
│   └── LinkageRepository.java
├── service/
│   └── LinkageService.java
├── util/
│   └── HashUtil.java
└── AadhaarLinkageApplication.java
```

---

## ⚙️ Prerequisites

Ensure the following are installed:

| Tool | Version | Description |
|------|----------|-------------|
| **Java** | 17+ | Required for Spring Boot 3.x |
| **Maven** | 3.8+ | Build automation tool |
| **Git** | Latest | To clone the repository |
| **Postman** | Optional | For API testing (alternative to Swagger UI) |

---

## 📦 Installation

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>
```

### 2️⃣ Build the Project

```bash
mvn clean install
```

### 3️⃣ Run the Application

```bash
mvn spring-boot:run
```

The application will start on:
```
http://localhost:8080
```

---

## 🧪 API Documentation (Swagger UI)

Once running, open in your browser:

👉 [http://localhost:8080/swagger-ui/index.html](http://localhost:8080/swagger-ui/index.html)

You will see:
```
POST /api/v1/person/manage
GET  /api/v1/person/health
```

---

## 📮 API Endpoints

### 1️⃣ Health Check
**GET** `/api/v1/person/health`
```json
"Person Linkage Service is running"
```

---

### 2️⃣ Manage Endpoint
**POST** `/api/v1/person/manage`

All actions are handled using the `action` field in the JSON request body.

---

### 🔹 INSERT Example
Creates a new identity record.
```json
{
  "action": "INSERT",
  "data": {
    "aadhaar_number": "123412341234",
    "pan_number": "ABCDE1234F",
    "voter_id": "XYZ1234567",
    "dl_number": "DL0420190001234",
    "forename": "Ravi",
    "secondname": "Kumar",
    "lastname": "Sharma",
    "dob": "1995-05-12",
    "address": "123 Main Street, Delhi",
    "gender": "Male"
  }
}
```

---

### 🔹 UPDATE Example
Updates an existing record using `oldAadhaarLinkageKey`.
```json
{
  "action": "UPDATE",
  "oldAadhaarLinkageKey": "ALK1234567890",
  "data": {
    "address": "456 New Colony, Mumbai",
    "lastname": "Verma"
  }
}
```

---

### 🔹 DELETE Example
Deletes a record using its unique linkage key.
```json
{
  "action": "DELETE",
  "oldAadhaarLinkageKey": "ALK1234567890"
}
```

---

### 🔹 SEARCH Example
Searches for a person by Aadhaar + DOB + Name combination.
```json
{
  "action": "SEARCH",
  "data": {
    "aadhaar_number": "123412341234",
    "dob": "1995-05-12",
    "forename": "Ravi",
    "lastname": "Sharma"
  }
}
```

---

## 🧠 Hashing Logic

All identity numbers and personal details are hashed using SHA-256 before being stored.  
Implemented in `HashUtil.java`:

```java
public static String sha256(String input) {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
    return Base64.getEncoder().encodeToString(hash);
}
```

---

## 🗃️ Database Configuration

By default, the application uses an **in-memory H2 database**.

You can open the console here:
```
http://localhost:8080/h2-console
```

JDBC URL:
```
jdbc:h2:mem:testdb
```

For production, switch to PostgreSQL or MySQL by editing `application.properties`:
```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/linkage_db
spring.datasource.username=postgres
spring.datasource.password=yourpassword
spring.jpa.hibernate.ddl-auto=update
```

---

## 🧰 Postman Testing

1. Open **Postman**  
2. Create a new **POST** request:  
   `http://localhost:8080/api/v1/person/manage`
3. Go to **Body → raw → JSON** and paste one of the payloads above.  
4. Hit **Send** 🚀

You’ll receive a JSON response with `status`, `message`, and `person` data.

---
or you can use swagger ui as testing at this url
http://localhost:8080/swagger-ui/index.html#/

## 🧩 Example Response (Insert)

```json
{
  "status": "SUCCESS",
  "message": "Record inserted successfully",
  "person": {
    "aadhaarLinkageKey": "ALK0987654321",
    "hashedAadhaarNumber": "xa8dD4s...",
    "hashedPanNumber": "bP9fX7c...",
    "gender": "Male"
  }
}
```

---

## 🧑‍💻 Tech Stack

| Component | Technology |
|------------|-------------|
| **Backend** | Spring Boot 3.x |
| **Database** | H2 (default) / PostgreSQL |
| **ORM** | Spring Data JPA |
| **Build Tool** | Maven |
| **API Docs** | Swagger (springdoc-openapi) |
| **Hashing** | SHA-256 (Java Security) |

---

## 🧩 Swagger Dependencies (in `pom.xml`)(already added)
Ensure you have:
```xml
<dependency>
    <groupId>org.springdoc</groupId>
    <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
    <version>2.5.0</version>
</dependency>
```

---




---

## 🧠 Notes

- Change `application.properties` for your local DB configuration.  
- Swagger UI auto-generates at runtime (no manual setup needed).  
- All sensitive fields are hashed — **no plain identity numbers are stored**.
