rm(list = ls())

# List of packages required
packages <- c("tidyverse", "DBI", "janitor", "reshape", "odbc", "lubridate", "keyring", "ROracle",
              "lmtest", "vars")

select <- dplyr::select
rename <- dplyr::rename

start_time <- Sys.time()  # Record start time

print(start_time)

# Check if each package is installed, install if not, then load
for (pkg in packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
  }
}

# set working directory
setwd("C:/Users/benjamint/OneDrive - The Gorilla Glue Company/Desktop/Power Automate/pos-v-shipments")
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
kroger_channels <- c("KROGER", "FRED MEYER")

print("start")

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
  select(-week_end_date, -sales, -qty) %>%
  group_by(entity, brand, updated_week_ending_date, forecast_channel, productline, producttype, productvariant, productform) %>%
  summarise(shipment_sales = sum(shipment_sales),
            shipment_units = sum(shipment_units)) %>%
  ungroup()

#x <- shipments %>% select(updated_week_ending_date) %>% distinct()
#xx <- shipments %>% filter(updated_week_ending_date == as.Date("2024-11-17"))

# pos 
pos <- DBI::dbGetQuery(con_snowflake, statement = read_file("sql_pulls/pos_sales.sql")) %>% 
  clean_names() %>%
  mutate(pos_sales = sales, pos_units = units, updated_week_ending_date = as.Date(updated_week_ending_date),
         customer = if_else(customer %in% kroger_channels, "KROGER", customer)) %>%
  select(-sales, -units)

#y <- pos %>% select(updated_week_ending_date) %>% distinct()
#yy <- pos %>% filter(updated_week_ending_date == as.Date("2024-11-17"))

# Convert forecast_channel and customer columns into vectors
forecast_channels <- shipments %>% select(forecast_channel) %>% distinct() %>% pull()
customers <- pos %>% select(customer) %>% distinct() %>% pull()

# Find the common values (matches) between the two vectors
matching_values <- intersect(forecast_channels, customers)

# Filter both tables before the join
shipments_filtered <- shipments %>%
  filter(forecast_channel %in% matching_values)

pos_filtered <- pos %>%
  filter(customer %in% matching_values) %>%
  mutate(brand = 'OKeeffes') %>%
  mutate(entity = 'The Gorilla Glue Company LLC')


# Perform the merge between pos and shipments based on multiple columns
merged_data <- shipments_filtered %>%
  full_join(pos_filtered, 
            by = c("brand",
                   "entity",
                   "forecast_channel" = "customer", 
                   "updated_week_ending_date", 
                   "productline", 
                   "producttype", 
                   "productvariant", 
                   "productform")) %>%
  select(-total_sales_by_customer, -customer_rank) 

write.csv(merged_data, paste0('output/ok_pos-v-shipments.csv'), row.names = FALSE)


join_keys <- c("brand", "entity", "forecast_channel", "updated_week_ending_date",
               "productline", "producttype", "productvariant", "productform")

# Find duplicated keys (both first and later duplicates)
duplicated_keys <- merged_data %>%
  group_by(across(all_of(join_keys))) %>%
  filter(n() > 1) %>%
  ungroup()

# Print the duplicated rows
print(duplicated_keys)

# Optional: how many duplicated key groups are there?
dup_key_count <- duplicated_keys %>%
  distinct(across(all_of(join_keys))) %>%
  nrow()

cat("Number of duplicated key groups: ", dup_key_count, "\n")

# QA
#e <- merged_data %>% filter(forecast_channel %in% matching_values) %>% summarise(shipment_sales = sum(shipment_sales, na.rm = TRUE), shipment_units = sum(shipment_units, na.rm = TRUE))
#ee <- shipments %>% filter(forecast_channel %in% matching_values) %>% summarise(shipment_sales = sum(shipment_sales, na.rm = TRUE), shipment_units = sum(shipment_units, na.rm = TRUE))
#combined_e <- rbind(e, ee)

# r <- pos %>% filter(customer %in% matching_values) %>% summarise(pos_sales = sum(pos_sales, na.rm = TRUE), pos_units = sum(pos_units, na.rm = TRUE))
# rr <- merged_data %>% filter(forecast_channel %in% matching_values) %>% summarise(pos_sales = sum(pos_sales, na.rm = TRUE), pos_units = sum(pos_units, na.rm = TRUE))
# combined_r <- rbind(r, rr)

run_granger_tests <- function(data, max_lag = 12, p_threshold = 0.05) {
  library(lmtest)
  library(dplyr)
  
  results <- list()
  group_vars <- c("brand", "forecast_channel", "productline")
  
  data_agg <- data %>%
    mutate(date = as.Date(updated_week_ending_date)) %>%
    group_by(across(all_of(group_vars)), date) %>%
    summarise(
      shipment_units = sum(shipment_units, na.rm = TRUE),
      pos_units = sum(pos_units, na.rm = TRUE),
      .groups = "drop"
    )
  
  data_grouped <- data_agg %>%
    group_by(across(all_of(group_vars))) %>%
    group_split()
  
  for (group_data in data_grouped) {
    key <- group_data %>%
      slice(1) %>%
      select(all_of(group_vars)) %>%
      as.list()
    
    df <- group_data %>%
      arrange(date) %>%
      drop_na()
    
    if (nrow(df) < (max_lag + 1)) {
      message("Skipping group: ", paste(key, collapse = " | "), 
              " — only ", nrow(df), " rows (needs at least ", max_lag + 1, ")")
      next
    }
    
    # Check for zero variance or constant series
    if (var(df$shipment_units) == 0 || var(df$pos_units) == 0) {
      message("Skipping group due to zero variance: ", paste(key, collapse = " | "))
      next
    }
    
    result <- tryCatch({
      gt1 <- grangertest(shipment_units ~ pos_units, order = max_lag, data = df)
      gt2 <- grangertest(pos_units ~ shipment_units, order = max_lag, data = df)
      
      tibble(
        brand = key$brand,
        forecast_channel = key$forecast_channel,
        productline = key$productline,
        pos_leads_ship_p = gt1$`Pr(>F)`[2],
        ship_leads_pos_p = gt2$`Pr(>F)`[2]
      )
    }, error = function(e) {
      message("Error in group ", paste(key, collapse = " | "), ": ", e$message)
      NULL
    })
    
    if (!is.null(result)) results[[length(results) + 1]] <- result
  }
  
  results_df <- bind_rows(results)
  results_df %>%
    filter(pos_leads_ship_p < p_threshold | ship_leads_pos_p < p_threshold)
}

# Usage:
granger_results <- run_granger_tests(data = merged_data)

granger_results_rounded <- granger_results %>%
  mutate(
    pos_leads_ship_p = round(pos_leads_ship_p, 2),
    ship_leads_pos_p = round(ship_leads_pos_p, 2)
  ) %>%
  mutate(
    leading_indicator = case_when(
      pos_leads_ship_p < 0.05 & ship_leads_pos_p >= 0.05 ~ "POS leads Shipments",
      ship_leads_pos_p < 0.05 & pos_leads_ship_p >= 0.05 ~ "Shipments leads POS",
      pos_leads_ship_p < 0.05 & ship_leads_pos_p < 0.05 ~ "Bidirectional",
      TRUE ~ "No significant causality"
    )
  )

write.csv(granger_results_rounded, paste0('output/granger_results_rounded.csv'), row.names = FALSE)

end_time <- Sys.time()  # Record end time
execution_time <- end_time - start_time  # Calculate the time difference

print(paste("Execution time:", execution_time))

print("done")



