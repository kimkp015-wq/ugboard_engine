from data.alerts import detect_missed_publish

if __name__ == "__main__":
    alert = detect_missed_publish()
    print(alert or "No alerts")