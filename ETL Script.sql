-- ETL Using T-SQL
-- Source Database is Northwnd. Target Database is Northwind_DW (empty tables)
-- Data Manipulations and Transofrmation 
-- Dim_Date table

Use Northwind_DW
go

--1 Function for ProductType column in Products table
Create Function fn_PrdType(@Prd_id int)
Returns VARCHAR(20)
As
Begin

Return
case when (select ProductUnitPrice from Northwind_DW.[dbo].[Dim_Products] where ProductBK = @Prd_id) >
(select AVG(ProductUnitPrice) from Northwind_DW.[dbo].[Dim_Products]) THEN 'Expensive' else 'Cheap'  end

End
go

--2 Function for Dim_Date table 
Create Function fn_Date(@Begin_Date Date,@End_Date Date)
returns table
as
RETURN  
(WITH cte AS
(
  SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1 AS [Incrementor]
  FROM   [master].[sys].[columns] sc1
  CROSS JOIN [master].[sys].[columns] sc2
)
SELECT 
       convert(int,convert(varchar(8),DATEADD(DAY, cte.[Incrementor], @Begin_Date),112)) as DateKey ,
	   DATEADD(DAY, cte.[Incrementor], @Begin_Date)  as [Date], 
       datepart(year,DATEADD(DAY, cte.[Incrementor], @Begin_Date)) as [Year],
	   datepart(QUARTER,DATEADD(DAY, cte.[Incrementor], @Begin_Date)) as [Quarter],
	   datepart(MONTH,DATEADD(DAY, cte.[Incrementor], @Begin_Date)) as [Month],
	   DATENAME(MONTH,DATEADD(DAY, cte.[Incrementor], @Begin_Date)) as [MonthName]

FROM   cte
WHERE  DATEADD(DAY, cte.[Incrementor], @Begin_Date) <= @End_Date)
go

----3 Procedure for the ETL
Create Procedure DW_Proc
As

--Truncate all the Tables in the Data Warehouse
Truncate table Northwind_DW.[dbo].[Dim_Customers]
Truncate table Northwind_DW.[dbo].[Dim_Employees]
Truncate table Northwind_DW.[dbo].[Dim_Orders]
Truncate table Northwind_DW.[dbo].[Dim_Products]
Truncate table Northwind_DW.[dbo].[Fact_Sales]
DROP TABLE IF EXISTS Northwind_DW.[dbo].[Dim_Date]
 
--1 Insert Dim_Customers
INSERT INTO Northwind_DW.[dbo].[Dim_Customers] (CustomerBK,CustomerName,City,Region,Country)
SELECT CustomerID, CompanyName, 
ISNULL(City,'Unknown'), 
ISNULL(Region,'Unknown'),
ISNULL(Country,'Unknown')
from NORTHWND.[dbo].[Customers]

--2 Insert Dim_Employees 
INSERT INTO Northwind_DW.[dbo].[Dim_Employees]
(EmployeeBK,LastName,FirstName,FullName,Title, BirthDate,Age,HireDate,Seniority,City,Country,Photo,ReportsTo)
SELECT EmployeeID, LastName, FirstName, FirstName + ' ' + LastName,
ISNULL(Title,'Unknown'),
ISNULL(BirthDate,'1900-1-1'),
ISNULL(year(GETDATE())-year(BirthDate),-1),
ISNULL(HireDate,'1900-1-1'),
ISNULL(year(GETDATE())-year(HireDate),-1),
ISNULL(City,'Unknown'),
ISNULL(Country,'Unknown'),
Photo,
ISNULL(ReportsTo,EmployeeID)
from NORTHWND.[dbo].[Employees]

--3 Insert Dim_Orders
INSERT INTO Northwind_DW.[dbo].[Dim_Orders] (OrderBK,ShipCity,ShipRegion,ShipCountry)
SELECT Orderid, 
ISNULL(ShipCity,'Unknown'),
ISNULL(ShipRegion,'Unknown'), 
ISNULL(ShipCountry,'Unknown')
from NORTHWND.[dbo].[Orders]

--4 Insert Dim_Products -- without ProductType Column
INSERT INTO Northwind_DW.[dbo].[Dim_Products]
(ProductBK,ProductName,ProductUnitPrice,CategoryName,SupplierName,Discontinued)
select ProductID, ProductName,
ISNULL(UnitPrice,-1),
CategoryName,CompanyName,Discontinued
from NORTHWND.[dbo].[Products] P
join NORTHWND.dbo.Categories C on P.CategoryID = C.CategoryID
join NORTHWND.dbo.Suppliers S on P.SupplierID = S.SupplierID

--5 Update ProductType Column
Update Northwind_DW.[dbo].[Dim_Products] 
SET ProductType =  Northwind_DW.[dbo].fn_PrdType(ProductBK)
from Northwind_DW.[dbo].[Dim_Products]

--6 Create Dim_Date
CREATE TABLE Northwind_DW.[dbo].[Dim_Date](
	[DateKey] int PRIMARY KEY NOT NULL,
	[Date] Date NOT NULL,
	[Year] int NOT NULL,
	[Quarter] int NOT NULL,
	[Month] int NOT NULL,
	[MonthName] [nvarchar](20) NOT NULL
)

--7 Insert Dim_Date
INSERT INTO Northwind_DW.[dbo].[Dim_Date] 
SELECT * from dbo.fn_Date('1996-01-01','1999-12-31')

--8 Insert Fact_Sales
INSERT INTO Northwind_DW.[dbo].[Fact_Sales]
SELECT OrderSK,ProductSK, DateKey,CustomerSK,EmployeeSK,UnitPrice,Quantity,Discount
from NORTHWND.dbo.[Order Details] OD 
join Northwind_DW.dbo.Dim_Orders DO on OD.OrderID = DO.OrderBK
join Northwind_DW.dbo.Dim_Products DP on OD.ProductID = DP.ProductBK
join NORTHWND.[dbo].[Orders] O on OD.OrderID = O.OrderID
join Northwind_DW.dbo.Dim_Date DD on O.OrderDate = DD.Date
join Northwind_DW.dbo.Dim_Customers DC on DC.CustomerBK = O.CustomerID
join Northwind_DW.dbo.Dim_Employees DE on DE.EmployeeBK = O.EmployeeID

go

--Execute Procedure 
Exec DW_Proc
go