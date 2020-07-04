package Beans

case class Login(timestamp: String,
                 process_timestamp: String,
                 browser_type: String, browser_version: String,
                 ip: String,
                 city: String,
                 user_name: String,
                 site_id: String,
                 channel: String,
                 site_domain: String,
                 site_create_name: String,
                 login_method: String,
                 page_num: Int,
                 backup_num: Int,
                 theme: String,
                 load_time: Long)

