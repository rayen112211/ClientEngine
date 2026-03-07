"""
Email Templates — Bilingual (Italian + English).
Auto-selects language based on location.

Using highly varied spintax {option1|option2} to bypass spam filters.
Using single, extremely simple and direct universal templates.
"""

SEQUENCE_STEP_NAMES = {
    1: "First Contact",
    2: "Social Proof",
    3: "Free Audit",
    4: "Quick Call",
    5: "Last Message",
}

# Italian cities/regions for language detection
ITALY_LOCATIONS = {
    "roma", "milano", "napoli", "torino", "palermo", "genova", "bologna",
    "firenze", "bari", "catania", "venezia", "verona", "messina", "padova",
    "trieste", "brescia", "parma", "modena", "reggio", "perugia", "cagliari",
    "livorno", "ravenna", "ferrara", "rimini", "siracusa", "sassari",
    "monza", "bergamo", "pescara", "trento", "vicenza", "terni", "novara",
    "piacenza", "ancona", "lecce", "udine", "salerno", "aosta", "como",
    "italy", "italia", "rome", "milan", "naples", "turin", "florence",
    "venice", "sicily", "sardinia", "calabria", "puglia", "toscana",
    "tuscany", "lazio", "lombardia", "lombardy", "campania", "emilia",
    "veneto", "piemonte", "piedmont", "liguria", "umbria", "marche",
    "abruzzo", "molise", "basilicata", "friuli",
}

def is_italy(location):
    """Check if location is in Italy."""
    if not location:
        return False
    words = location.lower().replace(",", " ").split()
    return any(w in ITALY_LOCATIONS for w in words)

def get_template(business_type, step, location=""):
    """
    Get email template. Auto-selects language:
    - Italy locations → Italian
    - Everything else → English
    Uses 'other' as the universal template set.
    """
    lang = "it" if is_italy(location) else "en"
    template = TEMPLATES.get((business_type, step, lang))
    if not template:
        template = TEMPLATES.get(("other", step, lang))
    return template

def get_all_templates():
    return {}

TEMPLATES = {
    # ── UNIVERSAL (ENGLISH) ───────────────────────────────────
    ("other", 1, "en"): {
        "subject_variants": [
            "{Quick question|Website idea|Quick thought|Improvement idea|A quick idea}",
            "{Your website|Your site|Question about your site|A quick question}",
        ],
        "body": "{Hi|Hello|Hey|Hi there},\n\n"
                "{I was looking at your site and I think it has a lot of potential to bring in more business.|I came across your site and thought I could help you get some better results from it.|I was just visiting your website and had an idea for it.}\n\n"
                "{If you're open to it, I can build a completely free prototype for you|I'd love to put together a free prototype showing how a new design would look|Would you be open to me creating a quick, free prototype of a new design}? {No strings attached|There's no cost|No obligation}, just want to show you what's possible.\n\n"
                "{Let me know if you are interested.|Sound interesting?|Let me know if I should put that together for you.}",
    },
    ("other", 2, "en"): {
        "subject_variants": [
            "{Following up|Just following up|Checking in}",
        ],
        "body": "{Hi|Hello|Hey},\n\n"
                "{Wanted to quickly follow up on my last email.|Just bumping this to the top of your inbox.|Checking to see if you saw my previous message.}\n\n"
                "{Like I mentioned, I'm happy to build a totally free prototype of a new website for you if you'd like to see it.|I'm still happy to create a free design mockup for you if you are interested.|I'd be happy to show you a free mockup of what an updated design would look like for you.}\n\n"
                "{Are you open to seeing what I can come up with?|Let me know if you would like to see it.|Just reply 'yes' and I'll get started.}",
    },
    ("other", 3, "en"): {
        "subject_variants": [
            "{Quick question|One last thing|A quick idea}",
        ],
        "body": "{Hi|Hello|Hey},\n\n"
                "{Not sure if you saw my last messages, but I really think a new site could help you out.|I don't want to spam you, just thought you'd be interested in seeing a free prototype.|Just reaching out once more about creating a free prototype for your site.}\n\n"
                "{If you want to see a free mockup, let me know. If not, no worries at all.|Let me know if you want to see what I can build, totally free.|Would love to show you a quick free design, if you are interested.}",
    },
    ("other", 4, "en"): {
        "subject_variants": [
            "{Checking in|Quick follow-up}",
        ],
        "body": "{Hi|Hello},\n\n{Looks like you are super busy. Just want to leave my portfolio here in case you ever need a new website in the future.|I'll leave it at this for now. If you ever want that free prototype, just reach out.}",
    },
    ("other", 5, "en"): {
        "subject_variants": [
            "{Closing the loop|Last message}",
        ],
        "body": "{Hi|Hello},\n\n{This will be my last email. If you ever decide you want a modern, fast website, let me know!|I won't bother you again, but feel free to reply if you ever need a site.}",
    },

    # ── UNIVERSAL (ITALIAN) ───────────────────────────────────
    ("other", 1, "it"): {
        "subject_variants": [
            "{Una domanda veloce|Idea per il sito web|Una proposta|Il tuo sito web}",
            "{Miglioramento del sito|Un'idea per te|Una domanda sul sito}",
        ],
        "body": "{Ciao|Buongiorno|Salve},\n\n"
                "{Ho visitato il tuo sito e penso che abbia un sacco di potenziale per portarti più clienti.|Dando un'occhiata al tuo sito credo che si possa fare un bel lavoro per attirare più persone.|Ho visto il tuo sito web e ho avuto un'idea per migliorarlo.}\n\n"
                "{Se ti interessa, posso prepararti un prototipo gratuito del nuovo sito|Mi piacerebbe mandarti un prototipo gratuito di come potrebbe diventare|Vorresti vedere una demo gratuita di un nuovo design}? {Senza alcun impegno|Nessun costo o obbligo|Tutto gratis e senza impegno}, solo per farti vedere cosa si può fare.\n\n"
                "{Fammi sapere se ti piacerebbe vederlo.|Ti interessa l'idea?|Rispondi a questa email se vuoi che te lo prepari.}",
    },
    ("other", 2, "it"): {
        "subject_variants": [
            "{Tutto bene?|Solo per aggiornarti|Veloce follow-up}",
        ],
        "body": "{Ciao|Buongiorno},\n\n"
                "{Ti scrivo solo per assicurarmi che tu abbia visto la mia email precedente.|Un veloce follow-up alla mia ultima email.|Ti riporto in cima questa email.}\n\n"
                "{Come ti dicevo, sono a disposizione per crearti un prototipo gratuito.|L'offerta del prototipo gratuito è sempre valida, se vuoi vedere la mia proposta.|Mi farebbe molto piacere mostrarti una demo gratuita.}\n\n"
                "{Vuoi che te lo prepari?|Fammi sapere se può interessarti.|Rispondi 'sì' e mi metto al lavoro.}",
    },
    ("other", 3, "it"): {
        "subject_variants": [
            "{Un'ultima domanda|Ancora un'idea|Prototipo sito web}",
        ],
        "body": "{Ciao|Salve},\n\n"
                "{Non voglio disturbarti troppo, volevo solo essere sicuro che ti fosse arrivata la mia proposta.|Immagino tu sia molto impegnato, ti scrivo un'ultima volta per il prototipo gratuito.|Ancora un veloce tentativo per vedere se ti interessa la demo gratuita.}\n\n"
                "{Se vuoi vedere la demo, scrivimi. Altrimenti, nessun problema!|Se la cosa non ti interessa, ignora pure questa email.|Fammi sapere, altrimenti non ti disturbo più.}",
    },
    ("other", 4, "it"): {
        "subject_variants": [
            "{Veloce aggiornamento|Sito web}",
        ],
        "body": "{Ciao|Salve},\n\n{Vedo che probabilmente sei molto preso in questo periodo. Ti lascio qui il link ai miei lavori, se in futuro vorrai un nuovo sito.|Questa è l'ultima volta che ti scrivo. Se in futuro avrai bisogno di un sito veloce e moderno, sai dove trovarmi.}",
    },
    ("other", 5, "it"): {
        "subject_variants": [
            "{Chiudo il cerchio|Ultima email}",
        ],
        "body": "{Ciao|Salve},\n\n{Non ti manderò altre email per non farti perdere tempo. Se mai cambierai idea, scrivimi pure!|Chiudo qui per non intasarti la casella. Sentiti libero di contattarmi se avrai bisogno in futuro.}",
    },
}
