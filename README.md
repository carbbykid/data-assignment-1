# Bài tập 1

# Giới thiệu Data Pipeline
Các bạn sẽ được yêu cầu code 1 data pipeline như sau:

**1.snapshot_user_info → 2.upload_file → 3.cloud_run**

1.  **batch_job/onprem_batch_job/snapshot_user_info.py**
    - Đẩy bảng `user_info` từ postgres onprem lên gcs `gs://mmo_adventure_event_processing/bronze-zone/user-info/user-info.json`
2. **batch_job/onprem_batch_job/**upload_event**.py**
    - upload folder `data` lên gcs `gs://mmo_adventure_event_processing/bronze-zone/event_info`
3. **batch_job/cloud_run_batch_job/main.py**
    - process data từ `gs://mmo_event_processing/bronze-zone/event` và ghi data thành file parquet partition theo cột year, month, day trong gs: `gs://mmo_event_processing/gold-zone/event/event_info`

# Cấu trúc file

```sql
.
├── Makefile
├── README.md
├── batch_job
│   ├── __init__.py
│   ├── cloud_run_batch_job
│   │   ├── Dockerfile
│   │   ├── cloudbuild.yaml
│   │   ├── main.py
│   │   └── requirements.txt
│   └── onprem_batch_job
│       ├── requirements.txt
│       ├── snapshot_user_info.py
│       └── upload_event.py
├── data
│   ├── 2023-08-12
│   ├── 2023-08-13
│   └── 2023-08-14
├── docker
│   ├── docker-compose.yml
│   └── postgres_local
│       ├── Dockerfile
│       └── init.sql
├── requirements.txt
├── terraform
│   ├── main.tf
│   └── variable.tf
└── tests
    ├── unit
    ├── intergration
    └── validiation
    
```

## Giải thích cấu trúc file

- Các Batch Job được lưu trữ trong folder batch_job.
    - cloud_run_batch_job: trữ job để chạy trên gcloud ( như đã giới thiệu trong ngày 3 )
    - onprem_batch_job: trữ 2 job để chạy onprem/local
- Data: trữ những file event data. Các bạn cần phải upload file này lên gcs
- docker: folder chứa môi trường ảo
    - postgre-local chứa Dockerfile CSDL Postgre local và data của bảng user_info.
    - docker-compose.yml : file config để startup môi trường ảo.
- terraform: folder chứa các file cần thiết của terraform.
- Makefile: file chứa các câu lệnh rút gọn để setup môi trường, deploy, chạy pipeline và data validation.
- tests: Bao gồm 3 loại, unit, integration và validation: 
  1. unit test: Dùng để test những hàm có chứa logic transform và không làm thay đổi hệ thống bên ngoài.
  1. integration: Dùng để test connection và logic hàm khi có connect với hệ thống bên ngoài. Ví dụ: Test connection và query bảng user_info.
  1. validation: Dùng để check chất lượng data sau khi chạy bằng các điều kiện, ràng buộc về business đối với data. ví dụ: có ít nhất 1 event trả về mỗi ngày.

## Makefile là gì ?

Makefile là một file chứa các câu lệnh `shell` và quy tắc để phần mềm `make` thực hiện quá trình biên dịch ( compile ) và build một project. 

Về cơ bản makefile có thể :  

1. Chia những câu lệnh `shell` theo block giống như một hàm. 
2. Định nghĩa thứ tự chạy và dependency của những block đó với nhau
3. Đặt tên biến môi trường.

Do đó mình chọn makefile để có thể đơn giản hoá việc build / tự động chạy và test project thay vì viết script `bash` hoặc `shell`. 

Ví dụ bạn muốn tạo môi trường ảo cho python đây là câu lệnh bạn sẽ sử dụng: 

 

```bash
python3 -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install -r requirements.txt
```

Khi bỏ vào Makefile: 

```bash
# Setup Variables
PYTHON = python3
VENV = .venv
VENV_BIN = $(VENV)/bin

# Make step
create_python_env:
		@$(PYTHON) -m venv $(VENV)

setup_python_env: create_python_env
		@$(VENV_BIN)/pip install -U pip
		@$(VENV_BIN)/pip install -r requirements.txt
```

Và lúc đó mình chỉ cần chạy: 

```bash
make setup_python_env
```

Make file sẽ chạy “hàm” create_python_env → setup_python_env.

Các bạn có thể tham khảo thêm về Makefile ở dây: 

- https://earthly.dev/blog/python-makefile/
- https://www.sas.upenn.edu/~jesusfv/Chapter_HPC_6_Make.pdf

## Terraform là gì ?

Terraform là công cụ Infrastructure as Code (IaC) cho phép định nghĩa và tự động triển khai hạ tầng cloud và on-prem.  

Terraform có khả năng tự động hoá quá trình tạo, cập nhật, tái sử dụng mã, và kiểm soát phiên bản, quản lý các tài nguyên trên nhiều cloud provider. 

Mình dùng terraform để đồng nhất quá trình tạo resource và cung cấp môi trường trên cloud. Quá trình này giúp mình test code dễ hơn. 

Terraform là công cụ cần thiết của các bạn DevOps

Các bạn có thể tham khảo thêm cách cấu hình terraform ở đây. 

- https://developer.hashicorp.com/terraform/tutorials/gcp-get-started


# Hướng dẫn làm bài

Các bạn sẽ thực hiện `code` ở giữa các phần hiển thị `#TODO: Begin` và `#TODO: End.`

---

Điểm số:

- 10 điểm cho mỗi 1 test. Bao gồm: 
  - 2 unit test
  - 1 integration test
  - 4 validation test public
  - 3 validation test ẩn

## Yêu cầu cài đặt

- Nếu là máy Windows thì cần cài đặt WSL2:
    - https://learn.microsoft.com/en-us/windows/wsl/install
- gcloud:
    - https://cloud.google.com/sdk/docs/install
- docker:
    - https://www.docker.com/
- terraform:
    - https://www.terraform.io/

Chạy lệnh sau để authenticate vào tài khoảng gcloud 

```bash
gcloud auth login
```

và 

```bash
gcloud auth application-default login
```

## Tạo môi trường ảo và setup thư viện
Tại folder assignment_1
Chỉnh tên project của mình tại file .makefile.env
Copy folder data vào folder assignment-1 (nơi chứa Makefile)
```bash
# .makefile.env
PROJECT_ID=<YOUR_PROJECT_ID> # Tên project id 
MEMORY=2G  # Memory cấp cho cloud run jobs
```

Chạy lệnh 
```bash
make setup  
```


## Cách chạy từng bước
1. **batch_job/onprem_batch_job/snapshot_user_info.py**
```bash
make snapshot_user_info 
```

2. **batch_job/onprem_batch_job/upload_file.py**
```bash
make upload_event 
```

3. **batch_job/cloud_run_batch_job/main.py**
```bash
make create_cloud_run_job
make trigger_cloud_run_job 
```

## Cách chạy end to end 
```bash
make run 
```

## Cách chạy test và validation sau khi đã xong job
Chạy từng test: 
```bash
make unit 
make integration
make validation
```
Lưu ý: nên chạy unit test và integration test trước khi chạy end to end 
Lưu ý: chỉ chạy validation sau khi cloud run job run thành công.

Chạy tất cả các test:

```bash
make test
```

## Tắt Docker Compose 
```bash
make docker_down
```

## Bật Docker Compose
```bash
make docker_up
```

## Clean môi trường 
```bash
make clean
```