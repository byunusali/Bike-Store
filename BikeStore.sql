
CREATE TABLE sales.JobAuditLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    JobName NVARCHAR(100),
    StepName NVARCHAR(100),
    RunStatus NVARCHAR(50),
    RunDateTime DATETIME DEFAULT GETDATE(),
    Message NVARCHAR(MAX)
);


CREATE TABLE sales.StoreKPI_History (
    HistoryID INT IDENTITY(1,1) PRIMARY KEY,
    StoreName NVARCHAR(100),
    TotalOrders INT,
    TotalRevenue DECIMAL(18,2),
    AOV DECIMAL(18,2),
    ReportDate DATETIME DEFAULT GETDATE()
);




USE msdb;
GO


EXEC sp_add_job 
    @job_name = N'Daily_Sales_Automation_Job';


EXEC sp_add_jobstep 
    @job_name = N'Daily_Sales_Automation_Job', 
    @step_name = N'Load_CSV_Data', 
    @subsystem = N'TSQL', 
    @command = N'TRUNCATE TABLE sales.order_items; 
                 BULK INSERT sales.order_items FROM ''C:\Data\order_items.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'');
                 INSERT INTO sales.JobAuditLog (JobName, StepName, RunStatus) VALUES (''Daily_Sales_Automation'', ''Load_CSV'', ''Success'');', 
    @retry_attempts = 2;


EXEC sp_add_jobstep 
    @job_name = N'Daily_Sales_Automation_Job', 
    @step_name = N'Calculate_KPIs', 
    @subsystem = N'TSQL', 
    @command = N'INSERT INTO sales.StoreKPI_History (StoreName, TotalOrders, TotalRevenue, AOV) EXEC sp_CalculateStoreKPI @StoreID = 1;';


EXEC sp_add_jobschedule 
    @job_name = N'Daily_Sales_Automation_Job', 
    @name = N'Daily_Schedule', 
    @freq_type = 4, 
    @freq_interval = 1, 
    @active_start_time = 010000;


EXEC sp_add_jobserver 
    @job_name = N'Daily_Sales_Automation_Job';




USE msdb;
GO


IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'Daily_Sales_Automation_Job')
BEGIN
    EXEC sp_delete_job @job_name = N'Daily_Sales_Automation_Job', @delete_unused_schedule = 1;
END
GO


create database task
use task

CREATE SCHEMA production;

CREATE SCHEMA sales;

CREATE TABLE production.categories (
    category_id INT IDENTITY (1, 1) PRIMARY KEY,
    category_name VARCHAR (255) NOT NULL
);


CREATE TABLE production.brands (
    brand_id INT IDENTITY (1, 1) PRIMARY KEY,
    brand_name VARCHAR (255) NOT NULL
);


CREATE TABLE production.products (
    product_id INT IDENTITY (1, 1) PRIMARY KEY,
    product_name VARCHAR (255) NOT NULL,
    brand_id INT NOT NULL,
    category_id INT NOT NULL,
    model_year SMALLINT NOT NULL,
    list_price DECIMAL (10, 2) NOT NULL,
    FOREIGN KEY (category_id) REFERENCES production.categories (category_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (brand_id) REFERENCES production.brands (brand_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE sales.customers (
    customer_id INT IDENTITY (1, 1) PRIMARY KEY,
    first_name VARCHAR (255) NOT NULL,
    last_name VARCHAR (255) NOT NULL,
    phone VARCHAR (25),
    email VARCHAR (255) NOT NULL,
    street VARCHAR (255),
    city VARCHAR (50),
    state VARCHAR (25),
    zip_code VARCHAR (5)
);




CREATE TABLE sales.stores (
    store_id INT IDENTITY (1, 1) PRIMARY KEY,
    store_name VARCHAR (255) NOT NULL,
    phone VARCHAR (25),
    email VARCHAR (255),
    street VARCHAR (255),
    city VARCHAR (255),
    state VARCHAR (10),
    zip_code VARCHAR (5)
);


CREATE TABLE sales.staffs (
    staff_id INT IDENTITY (1, 1)  PRIMARY KEY ,
    first_name VARCHAR (50) NOT NULL,
    last_name VARCHAR (50) NOT NULL,
    email VARCHAR (255) NOT NULL UNIQUE,
    phone VARCHAR (25),
    active TINYINT NOT NULL,
    store_id INT NOT NULL,
    manager_id INT,
    FOREIGN KEY (store_id) REFERENCES sales.stores (store_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (manager_id) REFERENCES sales.staffs (staff_id) ON DELETE NO ACTION ON UPDATE NO ACTION
);



CREATE TABLE sales.orders (
    order_id INT IDENTITY (1, 1) PRIMARY KEY,
    customer_id INT,
    order_status TINYINT, 
    order_date DATE,
    required_date DATE, 
    shipped_date DATE,
    store_id INT, 
    staff_id INT,
    FOREIGN KEY (customer_id) REFERENCES sales.customers (customer_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (store_id) REFERENCES sales.stores (store_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (staff_id) REFERENCES sales.staffs (staff_id) ON DELETE NO ACTION ON UPDATE NO ACTION
);

CREATE TABLE sales.order_items (
    order_id INT,
    item_id INT,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    list_price DECIMAL (10, 2) NOT NULL,
    discount DECIMAL (4, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY (order_id, item_id),
    FOREIGN KEY (order_id) REFERENCES sales.orders (order_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (product_id) REFERENCES production.products (product_id) ON DELETE CASCADE ON UPDATE CASCADE
);


CREATE TABLE production.stocks (
    store_id INT,
    product_id INT,
    quantity INT,
    PRIMARY KEY (store_id, product_id),
    FOREIGN KEY (store_id) REFERENCES sales.stores (store_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (product_id) REFERENCES production.products (product_id) ON DELETE CASCADE ON UPDATE CASCADE
);

BULK INSERT production.brands
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\brands.csv"
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

BULK INSERT production.categories
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\categories.csv"
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

BULK INSERT sales.customers
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\customers.csv"
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);


IF OBJECT_ID('tempdb..#orders_stage') IS NOT NULL
DROP TABLE #orders_stage;

CREATE TABLE #orders_stage (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_date VARCHAR(50),
    required_date VARCHAR(50),
    shipped_date VARCHAR(50),
    store_id VARCHAR(50),
    staff_id VARCHAR(50)
);


BULK INSERT #orders_stage
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\orders.csv"
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDQUOTE = '"',
    TABLOCK
);


ALTER TABLE sales.orders NOCHECK CONSTRAINT ALL;


INSERT INTO sales.orders (
    customer_id, order_status, order_date, required_date, shipped_date, store_id, staff_id
)
SELECT 
    CAST(TRIM(customer_id) AS INT),
    CAST(TRIM(order_status) AS TINYINT),
    CAST(TRIM(order_date) AS DATE),
    CAST(TRIM(required_date) AS DATE),
    CASE WHEN TRIM(shipped_date) IN ('NULL', '', '0') THEN NULL ELSE CAST(TRIM(shipped_date) AS DATE) END,
    CAST(TRIM(store_id) AS INT),
    CAST(TRIM(staff_id) AS INT)
FROM #orders_stage;


ALTER TABLE sales.orders CHECK CONSTRAINT ALL;

BULK INSERT sales.order_items
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\order_items.csv"
WITH (
    FIRSTROW = 2,           -- 1-qator (sarlavha)ni tashlab o'tish
    FIELDTERMINATOR = ',',  -- Ustunlar vergul bilan ajratilgan
    ROWTERMINATOR = '\n',   -- Qator oxiri (ba'zida '\r\n' bo'lishi mumkin)
    TABLOCK
);

select * from sales.order_items


BULK INSERT production.products
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\products.csv"
WITH (
    FIRSTROW = 2,           -- 1-qator (sarlavha)ni tashlab o'tish
    FIELDTERMINATOR = ',',  -- Ustunlar vergul bilan ajratilgan
    ROWTERMINATOR = '\n',   -- Qator oxiri (ba'zida '\r\n' bo'lishi mumkin)
    TABLOCK
);

select * from sales.staffs

BULK INSERT sales.staffs
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\staffs.csv"
WITH (
    FIRSTROW = 2,           -- 1-qator (sarlavha)ni tashlab o'tish
    FIELDTERMINATOR = ',',  -- Ustunlar vergul bilan ajratilgan
    ROWTERMINATOR = '\n',   -- Qator oxiri (ba'zida '\r\n' bo'lishi mumkin)
    TABLOCK
);


BULK INSERT production.stocks
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\stocks.csv"
WITH (
    FIRSTROW = 2,           -- 1-qator (sarlavha)ni tashlab o'tish
    FIELDTERMINATOR = ',',  -- Ustunlar vergul bilan ajratilgan
    ROWTERMINATOR = '\n',   -- Qator oxiri (ba'zida '\r\n' bo'lishi mumkin)
    TABLOCK
);


select * from production.products

BULK INSERT sales.stores
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\stores.csv"
WITH (
    FIRSTROW = 2,           -- 1-qator (sarlavha)ni tashlab o'tish
    FIELDTERMINATOR = ',',  -- Ustunlar vergul bilan ajratilgan
    ROWTERMINATOR = '\n',   -- Qator oxiri (ba'zida '\r\n' bo'lishi mumkin)
    TABLOCK
);



CREATE VIEW vw_BrandPerformance AS
SELECT 
    b.brand_name,
    SUM(oi.quantity) AS TotalUnitsSold,
    SUM(oi.quantity * oi.list_price * (1 - oi.discount)) AS BrandRevenue
FROM production.brands b
JOIN production.products p ON b.brand_id = p.brand_id
JOIN sales.order_items oi ON p.product_id = oi.product_id
GROUP BY b.brand_name;

select * from vw_BrandPerformance

CREATE VIEW vw_CustomerRetention AS
SELECT 
    COUNT(CASE WHEN OrderCount > 1 THEN 1 END) * 100.0 / COUNT(*) AS RepeatCustomerPercentage
FROM (
    SELECT customer_id, COUNT(order_id) AS OrderCount
    FROM sales.orders
    GROUP BY customer_id
) AS CustomerOrders;

select * from vw_CustomerRetention
-- 1. Vaqtinchalik jadval yaratish
CREATE TABLE #TempOrderItems (
    order_id INT,
    item_id INT,
    product_id INT,
    quantity INT,
    list_price DECIMAL(10,2),
    discount DECIMAL(4,2)
);

-- 2. CSV ni vaqtinchalik jadvalga yuklash
BULK INSERT #TempOrderItems
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\order_items.csv"
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');

-- 3. Faqat jadvalda yo'q ma'lumotlarni asosiy jadvalga o'tkazish
INSERT INTO sales.order_items (order_id, item_id, product_id, quantity, list_price, discount)
SELECT t.* FROM #TempOrderItems t
LEFT JOIN sales.order_items s ON t.order_id = s.order_id AND t.item_id = s.item_id
WHERE s.order_id IS NULL;

-- 4. Vaqtinchalik jadvalni o'chirish
DROP TABLE #TempOrderItems;


select * from sales.order_items

TRUNCATE TABLE sales.order_items;


INSERT INTO sales.order_items (order_id, item_id, product_id, quantity, list_price, discount)
SELECT 
    order_id, 
    item_id, 
    product_id, 
    quantity, 
    list_price, 
    discount
FROM #TempOrderItems AS t
WHERE NOT EXISTS (
    SELECT 1 
    FROM sales.order_items AS s 
    WHERE s.order_id = t.order_id AND s.item_id = t.item_id
);

-- 1. Foreign Key tekshiruvini o'chirish
ALTER TABLE sales.order_items NOCHECK CONSTRAINT ALL;

-- 2. Ma'lumotni yuklash (Vaqtinchalik jadvaldan yoki BULK INSERT orqali)
INSERT INTO sales.order_items (order_id, item_id, product_id, quantity, list_price, discount)
SELECT order_id, item_id, product_id, quantity, list_price, discount
FROM #TempOrderItems;

-- 3. Foreign Key tekshiruvini qayta yoqish
ALTER TABLE sales.order_items WITH CHECK CHECK CONSTRAINT ALL;




-- 1. Vaqtinchalik jadval yaratamiz
CREATE TABLE #TempStaffs (
    staff_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(25),
    active TINYINT,
    store_id INT,
    manager_id VARCHAR(10) -- Xatoni ushlash uchun vaqtincha VARCHAR qilamiz
);


-- 2. CSV dan vaqtinchalik jadvalga yuklash
BULK INSERT #TempStaffs
FROM "C:\Users\LegionGaming\Downloads\BikeStore - demo Project-20260204T200739Z-3-001\BikeStore - demo Project\staffs.csv"
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

-- 3. Faqat bazada yo'q xodimlarni (email bo'yicha) qo'shish
INSERT INTO sales.staffs (first_name, last_name, email, phone, active, store_id, manager_id)
SELECT 
    t.first_name, t.last_name, t.email, t.phone, t.active, t.store_id,
    CASE WHEN t.manager_id = 'NULL' OR t.manager_id = '' THEN NULL ELSE CAST(t.manager_id AS INT) END
FROM #TempStaffs t
WHERE NOT EXISTS (
    SELECT 1 FROM sales.staffs s WHERE s.active = t.active
);





INSERT INTO sales.orders (
    customer_id, 
    order_status, 
    order_date, 
    required_date, 
    shipped_date, 
    store_id, 
    staff_id
)
SELECT 
    CAST(customer_id AS INT),
    CAST(order_status AS TINYINT),
    CAST(order_date AS DATE),
    CAST(required_date AS DATE),
    -- shipped_date 'NULL' bo'lishi mumkinligini hisobga olamiz:
    CASE WHEN shipped_date = 'NULL' OR shipped_date = '' THEN NULL ELSE CAST(shipped_date AS DATE) END,
    CAST(store_id AS INT),
    CAST(staff_id AS INT)
FROM #orders_stage;




select * from #orders_stage


-- Xato berishi mumkin bo'lgan qatorlarni tekshirish
SELECT * FROM #orders_stage 
WHERE TRY_CAST(order_date AS DATE) IS NULL AND order_date IS NOT NULL;
CREATE PROCEDURE sp_CalculateStoreKPI
    @StoreID INT
AS
BEGIN
    SELECT 
        s.store_name,
        COUNT(DISTINCT o.order_id) AS TotalOrders,
        SUM(oi.quantity * oi.list_price * (1 - oi.discount)) AS TotalRevenue,
        AVG(oi.quantity * oi.list_price * (1 - oi.discount)) AS AverageOrderValue
    FROM sales.stores s
    LEFT JOIN sales.orders o ON s.store_id = o.store_id
    LEFT JOIN sales.order_items oi ON o.order_id = oi.order_id
    WHERE s.store_id = @StoreID
    GROUP BY s.store_name;
END;


CREATE PROCEDURE sp_GenerateRestockList
    @StoreID INT,
    @Threshold INT = 10
AS
BEGIN
    SELECT 
        p.product_name,
        st.quantity AS CurrentStock,
        @Threshold AS ThresholdLevel
    FROM production.stocks st
    JOIN production.products p ON st.product_id = p.product_id
    WHERE st.store_id = @StoreID AND st.quantity < @Threshold
    ORDER BY st.quantity ASC;
END;







CREATE PROCEDURE sp_GetCustomerProfile
    @CustomerID INT
AS
BEGIN
    -- Jami xarajat va buyurtmalar soni
    SELECT 
        c.first_name + ' ' + c.last_name AS CustomerName,
        COUNT(DISTINCT o.order_id) AS TotalOrders,
        SUM(oi.quantity * oi.list_price * (1 - oi.discount)) AS LifetimeSpend
    FROM sales.customers c
    JOIN sales.orders o ON c.customer_id = o.customer_id
    JOIN sales.order_items oi ON o.order_id = oi.order_id
    WHERE c.customer_id = @CustomerID
    GROUP BY c.first_name, c.last_name;

    -- Eng ko'p sotib olingan mahsulot (Top 3)
    SELECT TOP 3 
        p.product_name, 
        SUM(oi.quantity) AS QtyPurchased
    FROM sales.order_items oi
    JOIN sales.orders o ON oi.order_id = o.order_id
    JOIN production.products p ON oi.product_id = p.product_id
    WHERE o.customer_id = @CustomerID
    GROUP BY p.product_name
    ORDER BY QtyPurchased DESC;
END;





CREATE PROCEDURE sp_CompareSalesYearOverYear
    @Year1 INT,
    @Year2 INT
AS
BEGIN
    SELECT 
        YearGroup,
        SUM(TotalSales) AS YearlyRevenue
    FROM (
        SELECT 
            YEAR(order_date) AS YearGroup,
            SUM(quantity * list_price * (1 - discount)) AS TotalSales
        FROM sales.orders o
        JOIN sales.order_items oi ON o.order_id = oi.order_id
        WHERE YEAR(order_date) IN (@Year1, @Year2)
        GROUP BY YEAR(order_date)
    ) AS SalesData
    GROUP BY YearGroup;
END;
CREATE VIEW vw_StoreSalesSummary AS
SELECT 
    s.store_name,
    SUM(oi.quantity * oi.list_price * (1 - oi.discount)) AS Revenue,
    COUNT(DISTINCT o.order_id) AS NumberOfOrders,
    SUM(oi.quantity * oi.list_price * (1 - oi.discount)) / COUNT(DISTINCT o.order_id) AS AOV
FROM sales.stores s
JOIN sales.orders o ON s.store_id = o.store_id
JOIN sales.order_items oi ON o.order_id = oi.order_id
GROUP BY s.store_name;

CREATE VIEW vw_TopSellingProducts AS
SELECT 
    p.product_name,
    SUM(oi.quantity) AS TotalUnitsSold,
    SUM(oi.quantity * oi.list_price * (1 - oi.discount)) AS TotalRevenue,
    RANK() OVER (ORDER BY SUM(oi.quantity * oi.list_price * (1 - oi.discount)) DESC) AS SalesRank
FROM production.products p
JOIN sales.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_name;

CREATE VIEW vw_StaffPerformance AS
SELECT 
    st.first_name + ' ' + st.last_name AS StaffName,
    COUNT(o.order_id) AS OrdersHandled,
    SUM(oi.quantity * oi.list_price * (1 - oi.discount)) AS RevenueGenerated
FROM sales.staffs st
JOIN sales.orders o ON st.staff_id = o.staff_id
JOIN sales.order_items oi ON o.order_id = oi.order_id
GROUP BY st.first_name, st.last_name;

