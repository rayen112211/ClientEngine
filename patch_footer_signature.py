with open("app.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("add_unsubscribe_footer(body, token, APP_BASE_URL)", "add_unsubscribe_footer(body, lead_id)")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Replaced all instances of add_unsubscribe_footer signature bugs.")
