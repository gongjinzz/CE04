package Beans

case class Method(
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
                   method_name: String,
                   method_params: String,
                   method_time: Long
                 )
