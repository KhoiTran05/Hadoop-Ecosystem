import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
from datetime import datetime
from utils.logger_config import logger
import logging
import os
from typing import Dict, Any

# Cấu hình email (có thể đặt trong file .env hoặc biến môi trường)
EMAIL_CONFIG = {
    'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
    'smtp_port': int(os.getenv('SMTP_PORT', '587')),
    'sender_email': os.getenv('SENDER_EMAIL', 'your-email@gmail.com'),
    'sender_password': os.getenv('SENDER_PASSWORD', 'your-app-password'),
    'recipient_emails': os.getenv('RECIPIENT_EMAILS', 'admin@company.com').split(',')
}

# Cấu hình Kafka Consumer để lắng nghe topic sensor-alerts
consumer = KafkaConsumer(
    'sensor-alerts',
    bootstrap_servers=['localhost:9092'],
    group_id='email-alert-group',
    auto_offset_reset='latest',  # Chỉ lắng nghe các tin nhắn mới
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

class EmailAlertSender:
    """Class để gửi email cảnh báo"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.smtp_server = None
        
    def connect_smtp(self):
        """Kết nối đến SMTP server"""
        try:
            self.smtp_server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            self.smtp_server.starttls()
            self.smtp_server.login(self.config['sender_email'], self.config['sender_password'])
            logger.info("Đã kết nối thành công đến SMTP server")
            return True
        except Exception as e:
            logger.error(f"Lỗi kết nối SMTP: {e}")
            return False
    
    def disconnect_smtp(self):
        """Ngắt kết nối SMTP"""
        if self.smtp_server:
            try:
                self.smtp_server.quit()
                logger.info("Đã ngắt kết nối SMTP")
            except Exception as e:
                logger.error(f"Lỗi ngắt kết nối SMTP: {e}")
    
    def create_email_content(self, alert_data: Dict[str, Any]) -> tuple:
        city = alert_data.get('city', 'Unknown')
        country = alert_data.get('country', '')
        timestamp = alert_data.get('timestamp', '') 
        alert_type = alert_data.get('alert_type', 'UNKNOWN') 

        title_map = {
            "threshold": "WARNING: Exceeding Safety Limits",
            "change": "WARNING: Unusual Volatility"
        }
        email_subject = f"{title_map.get(alert_type, 'ALERT')}: {city}"
        
        timestamp_html = f"<li><strong>Time Detected:</strong> {timestamp}</li>" if alert_type == "threshold" else ""
        html_header = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <h2 style="color: #d9534f; border-bottom: 2px solid #d9534f; padding-bottom: 10px;">
                {title_map.get(alert_type, 'City Weather Alert')}
            </h2>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0;">
                <h3>Location & Time:</h3>
                <ul>
                    <li><strong>City:</strong> {city}, {country}</li>
                    <li><strong>Alert Type:</strong> {alert_type.upper() + " ALERT"}</li>
                    {timestamp_html}
                </ul>
            </div>
            
            <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; border: 1px solid #ffeeba;">
                <h3 style="margin-top: 0;">Warning details:</h3>
                <ul style="list-style-type: none; padding: 0;">
        """

        html_body_rows = ""
        
        anomalies = alert_data.get('anomalies', [])
        
        for item in anomalies:
            metric_name = item.get('metric', 'Unknown')
            current_val = item.get('value', 'N/A')
            message = item.get('message', '')
            ts = item.get("timestamp", "")
            
            time_html = f"<br/>• Time detected: <strong>{ts}</strong>" if alert_type == "change" else ""
            row_html = f"""
            <li style="background-color: #ffffff; margin-bottom: 10px; padding: 10px; border-left: 5px solid #dc3545; box-shadow: 1px 1px 3px rgba(0,0,0,0.1);">
                <strong style="color: #dc3545; font-size: 1.1em;">{metric_name}</strong>
                <div style="margin-top: 5px;">
                    • Recognized value: <strong>{current_val}</strong><br/>
                    • Condition: <span style="color: #a71d2a;">{message}</span>
                    {time_html}
                </div>
            </li>
            """
            html_body_rows += row_html

        html_footer = """
                </ul>
            </div>
            <p style="font-size: 12px; color: #666; margin-top: 20px;">
                This is an automated message from Spark Streaming Weather System.
            </p>
        </body>
        </html>
        """

        full_html = html_header + html_body_rows + html_footer
        
        return email_subject, full_html

    
    def send_email(self, alert_data: Dict[str, Any]) -> bool:
        """Gửi email cảnh báo"""
        try:
            alert_type = alert_data.get("alert_type")
            data = {
                "alert_type": alert_type,
                "city": alert_data.get("city"),
                "country": alert_data.get("country"),
                "anomalies": []
            }
            
            if alert_type == "threshold":
                data["timestamp"] = alert_data.get("event_timestamp")
                
                if alert_data["temp_anomaly"]:
                    temperature = alert_data.get("temperature")
                    data["anomalies"].append({
                        "metric": "Temperature",
                        "value": f"{temperature}°C",
                        "message": "Exceeding the safety threshold" if temperature > 0 else "Below the safety threshold"
                    })
                if alert_data["pressure_anomaly"]:
                    pressure = alert_data.get("pressure")
                    data["anomalies"].append({
                        "metric": "Pressure",
                        "value": f"{pressure}hPa",
                        "message": "Exceeding the safety threshold" if pressure >= 1033 else "Below the safety threshold"
                    })
                if alert_data["visibility_anomaly"]:
                    vis = alert_data.get("visibility")
                    data["anomalies"].append({
                        "metric": "Visibility",
                        "value": f"{vis}meter",
                        "message": "Below the safety threshold"
                    })
                if alert_data["wind_anomaly"]:
                    wind_spd = alert_data.get("wind_speed")
                    data["anomalies"].append({
                        "metric": "Wind Speed",
                        "value": f"{wind_spd}m/s",
                        "message": "Exceeding the safety threshold" 
                    })
                    
            elif alert_type == "change":
                temp_anomaly = alert_data["temp_change_anomaly"]
                wind_anomaly = alert_data["wind_change_anomaly"]
                press_anomaly = alert_data["pressure_change_anomaly"]
                humid_anomaly = alert_data["humidity_change_anomaly"]
                vis_anomaly = alert_data["visibility_change_anomaly"]
                
                if temp_anomaly:
                    if temp_anomaly == "rise rapidly":
                        value, timestamp, message = (alert_data.get("max_temp"), alert_data.get("max_temp_time"), 
                                                        f"Sudden increase of {alert_data.get("temp_rise")}°C in the last 30 minutes")
                    else:
                        value, timestamp, message = (alert_data.get("min_temp"), alert_data.get("min_temp_time"), 
                                                        f"Sudden decrease of {alert_data.get("temp_fall")}°C in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Temperature fluctuation",
                        "value": f"{value}°C",
                        "timestamp": timestamp,
                        "message": message
                    })
                if wind_anomaly:
                    value, timestamp, message = (alert_data.get("max_wind"), alert_data.get("max_wind_time"), 
                                                    f"Sudden increase of {alert_data.get("wind_rise")}m/s in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Wind speed fluctuation",
                        "value": f"{value}m/s",
                        "timestamp": timestamp,
                        "message": message
                    })
                if press_anomaly:
                    value, timestamp, message = (alert_data.get("min_pressure"), alert_data.get("min_pressure_time"), 
                                                    f"Sudden drop of {alert_data.get("pressure_drop")}hPa in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Pressure fluctuation",
                        "value": f"{value}hPa",
                        "timestamp": timestamp,
                        "message": message
                    })
                if humid_anomaly:
                    value, timestamp, message = (alert_data.get("max_humidity"), alert_data.get("max_humid_time"), 
                                                    f"Sudden increase of {alert_data.get("humidity_rise")}% in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Humidity fluctuation",
                        "value": f"{value}%",
                        "timestamp": timestamp,
                        "message": message
                    })
                if vis_anomaly:
                    value, timestamp, message = (alert_data.get("min_visibility"), alert_data.get("min_visibility_time"), 
                                                    f"Sudden drop to below 2000 meter in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Visibility fluctuation",
                        "value": f"{value}meter",
                        "timestamp": timestamp,
                        "message": message
                    })
            
            subject, html_content, text_content = self.create_email_content(alert_data)
            
            # Tạo message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.config['sender_email']
            msg['To'] = ', '.join(self.config['recipient_emails'])
            
            # Thêm nội dung text và HTML
            text_part = MIMEText(text_content, 'plain', 'utf-8')
            html_part = MIMEText(html_content, 'html', 'utf-8')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Gửi email
            if not self.smtp_server:
                if not self.connect_smtp():
                    return False
            
            self.smtp_server.send_message(msg)
            logger.info(f"Đã gửi email cảnh báo cho thiết bị {alert_data['device_id']} "
                       f"đến {len(self.config['recipient_emails'])} người nhận")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi gửi email: {e}")
            return False

def validate_email_config(config: Dict[str, Any]) -> bool:
    """Kiểm tra cấu hình email"""
    required_fields = ['smtp_server', 'smtp_port', 'sender_email', 'sender_password']
    for field in required_fields:
        if not config.get(field):
            logger.error(f"Thiếu cấu hình email: {field}")
            return False
    
    if not config.get('recipient_emails'):
        logger.error("Thiếu danh sách email người nhận")
        return False
    
    return True

def main():
    """Hàm chính để chạy email alert consumer"""
    print("🚀 Bắt đầu Email Alert Consumer...")
    print("📧 Đang lắng nghe các cảnh báo từ topic 'sensor-alerts'...")
    
    # Kiểm tra cấu hình email
    if not validate_email_config(EMAIL_CONFIG):
        print("❌ Cấu hình email không hợp lệ!")
        print("\nVui lòng cấu hình các biến môi trường sau:")
        print("- SMTP_SERVER (mặc định: smtp.gmail.com)")
        print("- SMTP_PORT (mặc định: 587)")
        print("- SENDER_EMAIL (email người gửi)")
        print("- SENDER_PASSWORD (mật khẩu ứng dụng)")
        print("- RECIPIENT_EMAILS (danh sách email người nhận, cách nhau bởi dấu phẩy)")
        return
    
    # Tạo email sender
    email_sender = EmailAlertSender(EMAIL_CONFIG)
    
    # Kết nối SMTP ban đầu
    if not email_sender.connect_smtp():
        print("❌ Không thể kết nối đến SMTP server!")
        return
    
    print(f"✅ Đã cấu hình gửi email từ: {EMAIL_CONFIG['sender_email']}")
    print(f"📬 Người nhận: {', '.join(EMAIL_CONFIG['recipient_emails'])}")
    
    try:
        alert_count = 0
        
        for message in consumer:
            alert_data = message.value
            alert_count += 1
            
            logger.info(f"📨 Nhận được cảnh báo #{alert_count} từ thiết bị {alert_data['device_id']}")
            
            # Gửi email
            success = email_sender.send_email(alert_data)
            
            if success:
                print(f"✅ Đã gửi email cảnh báo #{alert_count} thành công!")
            else:
                print(f"❌ Lỗi gửi email cảnh báo #{alert_count}")
                # Thử kết nối lại SMTP
                email_sender.disconnect_smtp()
                if email_sender.connect_smtp():
                    logger.info("Đã kết nối lại SMTP server")
    
    except KeyboardInterrupt:
        print("\n🛑 Dừng Email Alert Consumer...")
    except Exception as e:
        logger.error(f"Lỗi không mong đợi: {e}")
    finally:
        email_sender.disconnect_smtp()
        consumer.close()
        print("👋 Đã đóng kết nối")

if __name__ == "__main__":
    main()
