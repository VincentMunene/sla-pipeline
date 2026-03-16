import re


def search_or_default(pattern: str, text: str, default="UNKNOWN", flags=0):
    match = re.search(pattern, text, flags)
    return match.group(1).strip() if match else default


def extract_advanced_state_data(content: str, filename: str, folder_category: str) -> dict:
    """
    Extract metadata plus routing-based intervention timestamps/operators
    from one .prt file, following the notebook logic.
    """

    uetr_match = re.search(r'UETR\s*:\s*([a-f0-9\-]+)', content, re.IGNORECASE)
    msg_type_match = re.search(r'MX Input\s*:\s*([^\s\n]+)', content, re.IGNORECASE)
    sender_match = re.search(r'Requestor DN :.*?o=(.*?),', content, re.IGNORECASE)
    receiver_match = re.search(r'Responder DN :.*?o=(.*?),', content, re.IGNORECASE)
    service_match = re.search(r'Service Name\s*:\s*([^\s\n]+)', content, re.IGNORECASE)
    ref_match = re.search(r'InstructionIdentification:\s*([^\s\n\r]+)', content, re.IGNORECASE)
    amount_match = re.search(r'InterbankSettlementAmount:\s*([\d\.]+)', content, re.IGNORECASE)
    currency_match = re.search(r'Currency:\s*([A-Z]{3})', content, re.IGNORECASE)

    record = {
        "UETR": uetr_match.group(1) if uetr_match else "UNKNOWN",
        "Message_Type": msg_type_match.group(1) if msg_type_match else "UNKNOWN",
        "Sender_Bank": sender_match.group(1) if sender_match else "kcookena",
        "Receiver_Bank": receiver_match.group(1) if receiver_match else "UNKNOWN",
        "Service_Name": service_match.group(1) if service_match else "UNKNOWN",
        "Reference_Number": ref_match.group(1) if ref_match else "N/A",
        "Settlement_Amount": amount_match.group(1) if amount_match else "0",
        "Currency": currency_match.group(1) if currency_match else "UNK",
        "Filename": filename,
        "Folder": folder_category,
    }

    patterns = {
        "AUTH_SWIFT": r"_MP_authorisation\] to rp \[_SI_to_SWIFTNet\]",
        "SWIFT_AUTH": r"_AI_from_APPLI\] to rp \[_MP_authorisation\]",
        "AUTH_MOD": r"_MP_authorisation\] to rp \[_MP_mod_text\]",
        "MOD_AUTH": r"_MP_mod_text\] to rp \[_MP_authorisation\]",
        "CREATE_AUTH": r"_MP_creation\] to rp \[_MP_authorisation\]",
        "SWIFT_MOD": r"_AI_from_APPLI\] to rp \[_MP_mod_text\]",
    }

    routing_blocks = re.findall(
        r"Category\s*:\s*Routing.*?Creation Time\s*:\s*(.*?)\n.*?Operator\s*:\s*(.*?)\n.*?Text:\s*(.*?)(?=Category|Description|$)",
        content,
        re.DOTALL | re.IGNORECASE,
    )

    counters = {k: 0 for k in patterns.keys()}

    for time_str, operator, text in routing_blocks:
        clean_text = text.strip().replace("\n", " ")

        for key, regex in patterns.items():
            if re.search(regex, clean_text):
                counters[key] += 1
                n = counters[key]
                suffix = f"_{n}" if n > 1 else ""

                record[f"{key}_TIME{suffix}"] = time_str.strip()

                if key == "AUTH_SWIFT":
                    record[f"Authorizer{suffix}"] = operator.strip()

                if key == "AUTH_MOD":
                    record[f"Authorizer_to_mod{suffix}"] = operator.strip()

                if key == "MOD_AUTH":
                    record[f"Modifier{suffix}"] = operator.strip()

                if key == "CREATE_AUTH":
                    record[f"Creator{suffix}"] = operator.strip()

    return record