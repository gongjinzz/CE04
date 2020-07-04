package Beans

case class Error(
                  timestamp: String,
                  process_timestamp: String,
                  browser_type: String,
                  browser_version: String,
                  ip: String,
                  user_name: String,
                  site_id: String,
                  channel: String,
                  site_domain: String,
                  site_create_name: String,
                  page_link: String,
                  error_info: String
                )
