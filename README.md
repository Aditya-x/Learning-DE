## 📝 Learnings  
- When providing a **host** for a local **PostgreSQL** instance in Docker, use the **service name** instead of `"localhost"`.  
  - Example: If your PostgreSQL service is named `db` in `docker-compose.yml`, use `db` as the host instead of `localhost`.  