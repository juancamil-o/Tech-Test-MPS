# TECH-TEST-MPS

**AWS CDK with Python**, deploying AWS stacks oriented to ingestion (from SECOP II/Socrata API), processing and data querying using **S3, Glue, Athena, Lambda** y **Lake Formation**.

---

## Project Structure

```
.
├── athena/                
│   └── athena_stack.py
├── glue/                  
│   └── glue_stack.py
├── lambda_stack/          
│   └── lambda_stack.py
│   └── lambda_function.py
├── lakeformation/         
│   └── lakeformation_stack.py
├── s3/                   
│   └── s3_stack.py
├── tech_test_mps/         
│   └── tech_test_mps_stack.py
├── app.py                
├── requirements.txt       
├── requirements-dev.txt   
├── cdk.json               
├── README.md              
└── .gitignore
```

---

## Requirements

- **Python 3.11+**
- **Node.js 18+** (for CDK)
- **AWS CLI v2** (`aws configure`)
- **AWS CDK v2** installed:
  ```bash
  npm install -g aws-cdk
  ```

---

## Installation and environment

1. Clone repo and move to the folder:
   ```bash
   git clone <repo-url>
   cd TECH-TEST-MPS
   ```

2. Create virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   #  Linux/Mac
   .venv\Scripts\activate      #  Windows

   pip install -r requirements.txt
   ```

## Deploy

1. Bootstrap (first time):
   ```bash
   cdk bootstrap
   ```

2. Sintetize the stack (genera CloudFormation):
   ```bash
   cdk synth
   ```

3. Deploy all:
   ```bash
   cdk deploy
   ```

4. Deploy specific stack (ejemplo Athena):
   ```bash
   cdk deploy AthenaStack
   ```

---


## Security

- **Roles y permisos**: IAM roles are configured following principle of least privilege.
- **Lake Formation**: Permissions for Crawler and DB-level permissions for Athena (there are also table and column level permissons for Athena but can be run only after running once the crawler).
