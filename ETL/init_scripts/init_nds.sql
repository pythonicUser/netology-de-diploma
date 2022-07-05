CREATE SCHEMA IF NOT EXISTS supermarket_project; 

CREATE TABLE IF NOT EXISTS supermarket_project.Cities(
	city_id INTEGER PRIMARY KEY  NOT NULL UNIQUE,
	city_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS supermarket_project.Branches(
	branch_id INTEGER PRIMARY KEY NOT NULL UNIQUE,
	city_id INTEGER,
	branch_name CHAR(1) NULL UNIQUE,
	FOREIGN KEY (city_id) REFERENCES supermarket_project.cities(city_id)
);

CREATE TABLE IF NOT EXISTS supermarket_project.Customer_types(
	customer_type_id INTEGER PRIMARY KEY NOT NULL UNIQUE,
	customer_type VARCHAR(250) NOT NULL
);

CREATE TABLE IF NOT EXISTS supermarket_project.Payment_types(
	payment_type_id INTEGER PRIMARY KEY UNIQUE NOT NULL,
	payment_type VARCHAR(250)
);

CREATE TABLE IF NOT EXISTS supermarket_project.Product_lines(
	product_line_id INTEGER PRIMARY KEY UNIQUE NOT NULL,
	product_line VARCHAR(250)
);

CREATE TABLE IF NOT EXISTS supermarket_project.Invoices(
	invoice_id varchar(255) PRIMARY KEY UNIQUE NOT NULL,
	customer_type_id INTEGER NOT NULL,
	branch_id INTEGER NOT NULL,
	payment_type_id INTEGER,
	product_line_id INTEGER,
	gender VARCHAR(10) NOT NULL ,
	invoice_date TIMESTAMP NOT NULL,
	rating DECIMAL,
	quantity INTEGER NOT NULL,
	unit_price DECIMAL NOT NULL,
	created_at TIMESTAMP DEFAULT NOW(),
	FOREIGN KEY (customer_type_id) REFERENCES supermarket_project.customer_types(customer_type_id),
	FOREIGN KEY (branch_id) REFERENCES supermarket_project.branches(branch_id),
	FOREIGN KEY (payment_type_id) REFERENCES supermarket_project.payment_types(payment_type_id),
	FOREIGN KEY (product_line_id) REFERENCES supermarket_project.product_lines(product_line_id)
	);

INSERT INTO supermarket_project.cities
	VALUES
	(1, 'Yangon'),
	(2, 'Naypyitaw'),
	(3, 'Mandalay');

INSERT INTO supermarket_project.branches 
	VALUES 
	(1, 1, 'A'),
	(2, 3, 'B'),
	(3, 2, 'C');

INSERT INTO supermarket_project.customer_types 
	VALUES 
	(1, 'Member'),
	(2, 'Normal');

INSERT INTO supermarket_project.payment_types 
	VALUES 
	(1, 'Ewallet'),
	(2, 'Cash'),
	(3, 'Credit card');

INSERT INTO supermarket_project.product_lines 
	VALUES 
	(1, 'Health and beauty'),
	(2, 'Electronic accessories'),
	(3, 'Home and lifestyle'),
	(4, 'Sports and travel'),
	(5, 'Food and beverages'),
	(6, 'Fashion accessories');

CREATE MATERIALIZED VIEW IF NOT EXISTS supermarket_project.nds_mart AS
	select 
		i.invoice_id, 
		b.branch_name, 
		c.city_name,
		ct.customer_type,
		pt.payment_type,
		pl.product_line,
		i.gender,
		i.invoice_date,
		i.rating,
		i.quantity,
		i.unit_price,
		i.created_at as created_at_nds,
		i.quantity * i.unit_price AS cogs,
		(i.unit_price * i.quantity) * 0.05 as tax_5_perc,
		(i.unit_price * i.quantity) * 1.05 as total,
		(i.unit_price * i.quantity) * 1.05 - (i.quantity * i.unit_price) as gross_income,
		((i.unit_price * i.quantity) * 1.05 - (i.quantity * i.unit_price))/((i.unit_price * i.quantity) * 1.05) * 100 as gross_income_perc
		
	from supermarket_project.invoices i
	INNER JOIN supermarket_project.branches b
	ON i.branch_id = b.branch_id 
	INNER JOIN supermarket_project.cities c
	ON b.city_id = c.city_id
	INNER JOIN supermarket_project.customer_types ct
	ON i.customer_type_id = ct.customer_type_id
	INNER JOIN supermarket_project.payment_types pt
	ON i.payment_type_id = pt.payment_type_id
	INNER JOIN supermarket_project.product_lines pl
	ON i.product_line_id = pl.product_line_id;

create index on supermarket_project.nds_mart (branch_name);
create index on supermarket_project.nds_mart (payment_line);



