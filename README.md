## Key Takeaway:  
- When providing a **host** for a local **PostgreSQL** instance in Docker, use the **service name** instead of `"localhost"`.  
  - Example: If your PostgreSQL service is named `db` in `docker-compose.yml`, use `db` as the host instead of `localhost`.  


- ### PostgreSQL Case Sensitivity
  - PostgreSQL is **case-sensitive** when it comes to column names. This means that:
 If a column is stored as `"VendorID"`, referencing it as `vendorid` (lowercase) will **fail**.

  - If the column name is stored as lowercase, using `"VendorID"` (with quotes) will also **fail**.
  - example:
  ``` 
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'green_taxi_data';
