# -- APAN 5310: SQL & RELATIONAL DATABASES

#   -------------------------------------------------------------------------
#   --                                                                     --
#   --                            HONOR CODE                               --
#   --                                                                     --
#   --  I affirm that I will not plagiarize, use unauthorized materials,   --
#   --  or give or receive illegitimate help on assignments, papers, or    --
#   --  examinations. I will also uphold equity and honesty in the         --
#   --  evaluation of my work and the work of others. I do so to sustain   --
#   --  a community built around this Code of Honor.                       --
#   --                                                                     --
#   -------------------------------------------------------------------------


#     You are responsible for submitting your own, original work. We are
#     obligated to report incidents of academic dishonesty as per the
#     Student Conduct and Community Standards.


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------


# -- HOMEWORK ASSIGNMENT 6


#   NOTES:
#
#     - Type your code between the START and END tags. Code should cover all
#       questions. Do not alter this template file in any way other than
#       adding your answers. Do not delete the START/END tags. The file you
#       submit will be validated before grading and will not be graded if it
#       fails validation due to any alteration of the commented sections.
#
#     - Our course is using PostgreSQL. We grade your assignments in PostgreSQL.
#       You risk losing points if you prepare your SQL queries for a different
#       database system (MySQL, MS SQL Server, Oracle, etc).
#
#     - Make sure you test each one of your answers. If a query returns an error
#       it will earn no points.
#
#     - In your CREATE TABLE statements you must provide data types AND
#       primary/foreign keys (if applicable).
#
#     - You may expand your answers in as many lines as you find appropriate
#       between the START/END tags.
#


# -----------------------------------------------------------------------------


#
#  NOTE: Provide the script that covers all questions between the START/END tags
#        at the end of the file. Separate START/END tags for each question.
#        Add comments to explain each step.
#
#
#  QUESTION 1 (5 points)
#  ---------------------
#  For this assignment we will use a fictional dataset of customer transactions
#  of a pharmacy. The dataset provides information on customer purchases of
#  one or two drugs at different quantities. Download the dataset from the
#  assignment page.
#
#  You will notice that there can be multiple transactions per customer, each
#  one recorded on a separate row.
#
#  Design an appropriate 3NF relational schema. Then, create a new database
#  called "abc_pharmacy" in pgAdmin. Finally, provide the R code that
#  connects to the new database and creates all necessary tables as per the 3NF
#  relational schema you designed (Note: you should create more than one table).
#
#  NOTE: You should use pgAdmin ONLY to create the database. All other actions
#        must be performed in your R code. No points if the database
#        tables are created in pgAdmin and not with R code.

# -- START R CODE --
require('RPostgreSQL')
df <- read.csv('C:/Users/Huizhe ZHU/Desktop/APAN5310_HW6_DATA.csv')

## Load the PostgreSQL driver
drv <- dbDriver('PostgreSQL')

## Create a connection
con <- dbConnect(drv, dbname = 'abc_pharmacy',
                 host = 'localhost', port = 5432,
                 user = 'postgres', password = '123')

## Pass the SQL statements that create all tables
stmt <- "
    CREATE TABLE customers (
        customer_id   integer,
        first_name    varchar(50) NOT NULL,
        last_name     varchar(50) NOT NULL,
        email         varchar(50) NOT NULL,
        PRIMARY KEY (customer_id)
    );
    
        CREATE TABLE drugs (
        drug_id   integer,
        drug_name   varchar(200),
        drug_company   varchar(100) NOT NULL,
        PRIMARY KEY (drug_id)
    );

    CREATE TABLE orders (
        order_id   integer,
        customer_id   integer,
        purchase_timestamp  TIMESTAMP,
        drug_id integer,
        quantity    integer,
        price    money,
        PRIMARY KEY (order_id),
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
        FOREIGN KEY (drug_id) REFERENCES drugs (drug_id)
    );
    
    CREATE TABLE phone (
        customer_id   integer,
        phone    varchar(20),
        PRIMARY KEY (customer_id, phone),
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
    );
    
"


#stmt <- "
#DROP TABLE phone;
#DROP TABLE orders;
#DROP TABLE customers;
#"
## Execute the statement to create tables
dbGetQuery(con, stmt)


# -- END R CODE --

# -----------------------------------------------------------------------------
#
#  QUESTION 2 (15 points)
#  ---------------------
#  Provide the R code that populates the database with the data from the
#  provided "APAN5310_HW6_DATA.csv" file. You can download the dataset
#  from the assignment page. It is anticipated that you will perform several steps
#  of data processing in R in order to extract, transform and load all data from
#  the file to the database tables. Manual transformations in a spreadsheet, or
#  similar, are not acceptable, all work must be done in R. Make sure your code
#  has no errors, no partial credit for code that returns errors. When grading,
#  we will run your script and test your work with sample SQL scripts on the
#  database that will be created and populated.
#

# -- START R CODE --
# table 1: customers
customers_df <- unique(df[c('first_name', 'last_name', 'email')])    # 949 unique customers
customers_df$customer_id <- 1:nrow(customers_df)
dbWriteTable(con, name="customers", value=customers_df, row.names=FALSE, append=TRUE)


#--------------------------------------------------------------------------------------
# table 2: drugs
# extract first drug info
drug1 <- df[c('drug_name_1','drug_company_1')]
drug2 <- df[c('drug_name_2','drug_company_2')]

# unify column names
names(drug1)[names(drug1) == "drug_name_1"] <- "drug_name"
names(drug1)[names(drug1) == "drug_company_1"] <- "drug_company"
names(drug2)[names(drug2) == "drug_name_2"] <- "drug_name"
names(drug2)[names(drug2) == "drug_company_2"] <- "drug_company"

drugs_df <- rbind(drug1,drug2)
drugs_df <- drugs_df[!drugs_df$drug_company=="",]
drugs_df <- unique(drugs_df)
drugs_df$drug_id <- 1:nrow(drugs_df)

dbWriteTable(con, name="drugs", value=drugs_df, row.names=FALSE, append=TRUE)


#--------------------------------------------------------------------------------------
# table 3: orders

##1. map customer_id to transaction table 
customer_id_list <- sapply(df$email, function(x) customers_df$customer_id[customers_df$email == x])
df$customer_id <- customer_id_list

##2. map drug_id to transaction table 

tem_drug1_df <- df[c('customer_id','drug_name_1','drug_company_1','quantity_1', 'price_1', 'purchase_timestamp')]
tem_drug2_df <- df[c('customer_id','drug_name_2','drug_company_2','quantity_2', 'price_2', 'purchase_timestamp')]
tem_drug2_df <- tem_drug2_df[complete.cases(tem_drug2_df),]  ## remove rows contains na in tem_drug2_df

# change column names for drug1
names(tem_drug1_df)[names(tem_drug1_df) == "drug_name_1"] <- "drug_name"
names(tem_drug1_df)[names(tem_drug1_df) == "quantity_1"] <- "quantity"
names(tem_drug1_df)[names(tem_drug1_df) == "price_1"] <- "price"
names(tem_drug1_df)[names(tem_drug1_df) == "drug_company_1"] <- "drug_company"
# change column names for drug2
names(tem_drug2_df)[names(tem_drug2_df) == "drug_name_2"] <- "drug_name"
names(tem_drug2_df)[names(tem_drug2_df) == "quantity_2"] <- "quantity"
names(tem_drug2_df)[names(tem_drug2_df) == "price_2"] <- "price"
names(tem_drug2_df)[names(tem_drug2_df) == "drug_company_2"] <- "drug_company"

# merge drug 1 and drug 2
tem_order_df <- rbind(tem_drug1_df,tem_drug2_df)

# map drug_id to tem_order_df 
tem_order_df1 <- merge(tem_order_df, drugs_df)
order_df <- tem_order_df1[c('customer_id', 'drug_id', 'quantity','price','purchase_timestamp')]

# add order_id to order_df
order_df$order_id <- 1:nrow(order_df)
dbWriteTable(con, name="orders", value=order_df, row.names=FALSE, append=TRUE)


#--------------------------------------------------------------------------------------
# table 4: phone
## 1. split phone number into two
s_phone <- strsplit(as.character(df$cell_and_home_phones),';')

# Create a new expanded dataframe
phone_df <- data.frame(customer_id = rep(df$customer_id, sapply(s_phone, length)),
                 phone = unlist(s_phone))
nrow(phone_df)
phone_df <- unique(phone_df)

dbWriteTable(con, name="phone", value=phone_df, row.names=FALSE, append=TRUE)


# -- END R CODE --

# -----------------------------------------------------------------------------
#
#  QUESTION 3 (2 points)
#  ---------------------
#  Write the R code that queries the "abc_pharmacy" database and returns
#  the customer name(s) and total purchase cost of the top 3 most expensive
#  transactions.
#
#  Type the actual result as part of your answer below, as a comment.
#

# -- START R CODE --

stmt <- "
SELECT full_name, total_transaction FROM 
(SELECT customer_id, total_transaction, RANK() OVER (ORDER BY total_transaction DESC) AS Ranking 
FROM (
SELECT customer_id, SUM(quantity*price) AS total_transaction 
FROM orders
GROUP BY (customer_id, purchase_timestamp)
      ) AS FOO) AS LOO 
	  
NATURAL JOIN (
SELECT customer_id, first_name ||' '|| last_name AS full_name
FROM customers) AS TOO
WHERE ranking <=3; 

"

# Execute the statement and store results in a temporary dataframe
temp_df <- dbGetQuery(con, stmt)

# Show results: 
temp_df

#----the top3 customers are:------- 
# 1. Lotty Kubica           $192.11
# 2. Cynthy Gorwood         $181.91
# 3. Eleni Devita           $179.54


# -- END R CODE --

# -----------------------------------------------------------------------------
#
#  QUESTION 4 (5 points)
#  ---------------------
#  Write the R code that queries the "abc_pharmacy" database and creates a
#  histogram of drug prices (price on the x-axis, frequency on the
#  y-axis). The figure must have proper axis titles, title and legend.
#
#  Result should be one figure, do not produce separate figures.
#

# -- START R CODE --

stmt = "SELECT * FROM orders;"
orders <- dbGetQuery(con, stmt)

price <- gsub(pattern = '\\$',
     replacement = '',
     x = orders$price)

hist(as.numeric(price),
     main="Drug Prices Histogram Plot",
     xlab="Price", 
     ylab="Frequency",
     border="blue"
     )

### since it is a histogram of a single variable 'price', it is not necessary to add a legend. Title is more critical. 

# -- END R CODE --

# -----------------------------------------------------------------------------
