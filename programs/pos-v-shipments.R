rm(list = ls())

# List of packages required
packages <- c("tidyverse", "DBI", "janitor", "reshape", "odbc", "lubridate", "keyring", "ROracle")


# Check if each package is installed, install if not, then load
for (pkg in packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
  }
}

# set working directory
setwd('C:/Users/benjamint/R Projects/pos-v-shipments')
# get directory
getwd()

# connections
con_sql <- DBI::dbConnect(odbc(), Driver = "SQL Server", Server = "ggjet01",
                          Database = "JetGpDwh", Port = 1433)

con_snowflake <- dbConnect(odbc::odbc(), dsn="PBI")

# con_snowflake <- DBI::dbConnect(odbc::odbc(), 
#                                 Driver = "SnowflakeDSIIDriver", 
#                                 Server = "gg2.east-us-2.azure.snowflakecomputing.com", 
#                                 uid = keyring::key_list("SNOWFLAKE")[1,2], 
#                                 pwd = keyring::key_get("SNOWFLAKE", "BENJAMINTHURBER"), 
#                                 Warehouse = "POS_LOADING_TEST_WH")

drv<- dbDriver("Oracle")
connect.string <- 'oax0831185281_low'
con_adw <- dbConnect(drv, username ="faw_pbi_custom", password="mrPqbHZIgEf2gO3",
                     dbname = connect.string)

walmart_channels <- c("WALMART OTH US", "WMT D02 HBL US", "WMT D12 PNT US", "WMT D19 CFT US")

# shipments
shipments <- DBI::dbGetQuery(con_adw, statement = read_file("sql_pulls/oracle_shipments.sql")) %>% 
  clean_names() %>%
  mutate(
    # Convert week_end_date to proper Date format (YYYY-MM-DD)
    updated_week_ending_date = as.Date(week_end_date),
    
    # Trim and group forecast_channel
    forecast_channel = str_trim(forecast_channel),
    forecast_channel = if_else(forecast_channel %in% walmart_channels, "WALMART US", forecast_channel),
    shipment_sales = sales,
    shipment_units = qty) %>%
  select(-week_end_date, -sales, -qty)

# pos 
pos <- DBI::dbGetQuery(con_snowflake, statement = read_file("sql_pulls/pos_sales.sql")) %>% 
  clean_names() %>%
  mutate(pos_sales = sales, pos_units = units) %>%
  select(-sales, -units)

# Convert forecast_channel and customer columns into vectors
forecast_channels <- shipments %>% select(forecast_channel) %>% distinct() %>% pull()
customers <- pos %>% select(customer) %>% distinct() %>% pull()

# Find the common values (matches) between the two vectors
matching_values <- intersect(forecast_channels, customers)


# Perform the merge between pos and shipments based on multiple columns
merged_data <- shipments %>%
  inner_join(pos, 
            by = c("forecast_channel" = "customer", 
                   "updated_week_ending_date", 
                   "productline", 
                   "producttype", 
                   "productvariant", 
                   "productform")) %>%
  select(-total_sales_by_customer, -customer_rank)

write.csv(merged_data, paste0('output/ok_pos-v-shipments.csv'), row.names = FALSE)
