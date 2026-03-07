<div align="center">
  <img src="https://via.placeholder.com/150x150.png?text=⚡" alt="ClientEngine Logo" width="100"/>
  <h1>ClientEngine</h1>
  <p><strong>A Localised, AI-Heuristic Cold Email Client Acquisition System</strong></p>
  
  <p>
    <a href="https://github.com/rayen112211/ClientEngine/issues"><img src="https://img.shields.io/github/issues/rayen112211/ClientEngine" alt="Issues"></a>
    <a href="https://github.com/rayen112211/ClientEngine/network/members"><img src="https://img.shields.io/github/forks/rayen112211/ClientEngine" alt="Forks"></a>
    <a href="https://github.com/rayen112211/ClientEngine/stargazers"><img src="https://img.shields.io/github/stars/rayen112211/ClientEngine" alt="Stars"></a>
    <img src="https://img.shields.io/badge/Python-3.9+-blue.svg" alt="Python Version">
    <img src="https://img.shields.io/badge/Flask-Web%20Framework-lightgrey.svg" alt="Flask">
  </p>
</div>

---

## 🚀 Overview

**ClientEngine** is a fully automated, local Python application designed to revolutionize outbound client acquisition. 

Instead of relying on generic email blasts that result in 0% conversion, ClientEngine uses **intelligent lead qualification**, **dynamic business-type routing**, **multi-touch automated sequencing**, and **real-time SMTP validation** to connect with high-intent leads that actually convert.

> *From 400 cold emails = 0 clients ➡️ Intelligent targeting = Real conversions.*

## ✨ Key Features

- 🧠 **AI-Heuristic Lead Scoring (0-100)**: Automatically scrapes business websites and ranks leads based on digital footprint (e.g., missing SSL, slow load times, high Google Ratings with poor websites).
- 🔀 **Dynamic Business-Type Routing**: Maps leads to 7 predefined business types (Restaurants, B2B, Ecommerce, etc.) and injects highly personalized, industry-specific pain points.
- 🛡️ **Real-Time SMTP Bounce Protection**: Conducts secret SMTP handshakes with target Mail Exchanges (MX) to verify inbox existence *before* sending, drastically reducing 550 Hard Bounces and protecting sender reputation.
- ⏳ **Smart Rate-Limit Auto-Pause**: Automatically detects provider limits (e.g., SpaceMail 500/hr limit) and gracefully pauses pipelines for 61 minutes before resuming.
- 📁 **Native IMAP Sync**: Bypasses typical script limitations by directly pushing dispatched emails into your email provider's `Sent` folder via an IMAP background thread.
- 🔄 **5-Step Automated Sequence**: Pre-built logic for automated follow-ups spanning 21 days with varying delays and intelligent A/B subject line rotation.
- 📊 **Beautiful Local Dashboard**: Built with Vanilla JS and CSS, providing real-time pipeline status, analytics, and conversion tracking without relying on external SaaS.

## 🛠️ Architecture

ClientEngine runs entirely locally on your machine.

*   **Backend Engine**: Python & Flask
*   **Database**: SQLite (Zero configuration needed)
*   **Frontend**: Vanilla HTML/JS/CSS (No heavy frameworks)
*   **Data Sources**: CSV Import + Google Places API integration for localized enrichment.
*   **Mail Protocol**: Native `smtplib` and `imaplib` pipelines.

## 🚦 Quick Start Guide

### 1. Prerequisites
Ensure you have [Python 3.9+](https://www.python.org/downloads/) installed on your system.

### 2. Installation
Clone the repository and install the required dependencies:
```bash
git clone https://github.com/rayen112211/ClientEngine.git
cd ClientEngine
pip install -r requirements.txt
```

### 3. Configuration
Copy the environment template and add your credentials:
```bash
cp .env.example .env
```
Edit `.env` and configure your SMTP variables:
```env
SMTP_HOST=mail.yourprovider.com
SMTP_PORT=587
SMTP_USER=hello@yourdomain.com
SMTP_PASSWORD=your_secure_password
SMTP_USE_SSL=false
```

### 4. Running the Application
**On Windows:** Simply double-click `Start_ClientEngine.bat`.

**Via Terminal:**
```bash
python run.py
```
Open your browser and navigate to `http://localhost:5000`.

## 📈 System Workflow

1. **Import:** Upload raw datasets (CSV) of local businesses.
2. **Enrich & Score:** The engine visits the websites, identifies weaknesses (no mobile optimization, broken links), and assigns a qualification score (Tier 1, 2, or 3).
3. **Drafting:** Leads scoring 60+ are routed into automated pipelines based on their vertical.
4. **Dispatching:** Emails are sent sequentially with random human-like delays (60-180s).
5. **Follow-Up:** The background daemon wakes up daily to dispatch day 3, 7, 14, and 21 automated follow-ups to non-repliers.

## 🛡️ Anti-Spam Architecture
ClientEngine is built to protect your domain reputation:
*   Sends 100% Plain Text emails for maximum deliverability.
*   Randomized sleep intervals to mimic human typing/sending patterns.
*   Spintax support `{Hi|Hello|Hey}` and automatic variable injection.
*   No bulk BCC sending (each email is a unique 1-to-1 thread).

## 👨‍💻 Author
**Rayen Lazizi**
*   Portfolio: [rayenlazizi.tech](https://rayenlazizi.tech)
*   GitHub: [@rayen112211](https://github.com/rayen112211)

---
*If you find this project useful, please consider giving it a ⭐ on GitHub!*
