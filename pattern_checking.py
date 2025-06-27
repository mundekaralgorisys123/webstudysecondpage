import re

def extract_metals(text):
    """Extracts and prioritizes longest, most descriptive metal-related phrases."""
    if not text:
        return []

    text_upper = text.upper()

    patterns = [
        # Match like "9K White Gold", "14CT Rose Gold & White Gold"
        r'\b\d{1,2}(?:K|CT|CARAT)\s*(?:WHITE|YELLOW|ROSE|STRAWBERRY|TWO[- ]TONE)?\s*GOLD\b',

        # Match standalone metal names
        r'\b(?:PLATINUM|STERLING\s*SILVER|SILVER|WHITE\s*GOLD|YELLOW\s*GOLD|ROSE\s*GOLD|STRAWBERRY\s*GOLD|TWO[- ]TONE\s*GOLD|TITANIUM|BRASS|PALLADIUM|COPPER|ALLOY)\b',

        # Match just karat values like "9K", "14CT" — only if not part of a decimal
        r'(?<![\d.])\b\d{1,2}(?:K|CT|CARAT)\b'
    ]

    all_matches = []
    for pattern in patterns:
        all_matches.extend(re.findall(pattern, text_upper))

    # Remove duplicates and substrings
    unique_matches = sorted(set(all_matches), key=len, reverse=True)
    final_matches = []
    for match in unique_matches:
        if not any(match in longer for longer in final_matches):
            final_matches.append(match)

    return final_matches


def extract_kt_value(text):
    """Extract Kt value or base metal type from product text. Returns lowercase string."""
    if not text:
        return None

    metals = extract_metals(text)
    if metals:
        value = metals[0].upper()

        # Normalize named metals
        for metal in ['PLATINUM', 'STERLING SILVER', 'SILVER', 'TITANIUM', 'BRASS', 'PALLADIUM', 'COPPER', 'ALLOY']:
            if metal in value:
                return metal.lower()

        # Normalize spacing and symbols
        if '&' in value:
            parts = [p.strip() for p in value.split('&')]
            value = ' & '.join(parts)

        # Fix concatenated names
        value = value.replace('WHITEGOLD', 'WHITE GOLD')
        value = value.replace('YELLOWGOLD', 'YELLOW GOLD')
        value = value.replace('ROSEGOLD', 'ROSE GOLD')
        value = value.replace('TWOTONE', 'TWO-TONE GOLD')
        value = value.replace('TWO TONE', 'TWO-TONE')

        # Convert CT/CARAT to K
        value = re.sub(r'(\d{1,2})(CT|CARAT)', r'\1K', value)
        value = re.sub(r'(\d{1,2}K)([A-Z])', r'\1 \2', value)

        return value.lower()

    # 🔍 Fallback: check for "Diamond <Metal>" structure
    diamond_metal_match = re.search(
        r'\bDIAMOND\s+(PLATINUM|STERLING\s+SILVER|SILVER|TITANIUM|BRASS|PALLADIUM|COPPER|ALLOY)\b',
        text.upper()
    )
    if diamond_metal_match:
        return diamond_metal_match.group(1).lower()

    # ✅ Final fallback: If "diamond" is in the text but no metal found
    if "DIAMOND" in text.upper():
        return "diamond"

    return None

def extract_diawt_value(text):
    """Extract smallest valid diamond weight (ct), preserving 'tw' and handling formats like 0,50 ct"""
    if not text:
        return None

    text = str(text).upper()

    # Convert European decimal (comma) to dot
    text = text.replace(',', '.')

    # Remove metal descriptors only if at start
    metal_free_text = re.sub(
        r'^\s*\d{1,2}(?:K|CT|CARAT)(?:\s*(?:[A-Z]+\s*&\s*[A-Z]+|ROSE|WHITE|YELLOW|STRAWBERRY|TWO-TONE)\s*GOLD)?\s*',
        '',
        text,
        flags=re.IGNORECASE
    )

    if any(x in metal_free_text for x in ['CUBIC ZIRCONIA', 'SAPPHIRE', 'CREATED']):
        return None

    # Match patterns: 1-3/4, 3/4, 0.25, 0,50, 1.25, etc. with ct indicators
    matches = re.findall(
        r'(\d+-\d+/\d+|\d+/\d+|\d*\.\d+|\d+)\s*(CTW|CT\s*TW|CT|CARAT\s*TW|CARAT|CT\.*\s*T*W*\.?)',
        metal_free_text,
        re.IGNORECASE
    )

    diamond_cts = []
    for val, unit in matches:
        ct_val = parse_ct(val)
        if ct_val is not None and ct_val < 5.0:
            diamond_cts.append((val.strip(), unit.strip(), ct_val))

    if not diamond_cts:
        return None

    # Pick the smallest valid ct
    smallest = min(diamond_cts, key=lambda x: x[2])
    return standardize_diawt_value(f"{smallest[0]} {smallest[1]}")
    

def parse_ct(val):
    """Convert ct string to float, supporting composite fractions like 1-3/4"""
    try:
        if '-' in val and '/' in val:
            whole, frac = val.split('-')
            num, denom = frac.split('/')
            return int(whole) + float(num) / float(denom)
        if '/' in val:
            num, denom = val.split('/')
            return float(num) / float(denom)
        return float(val)
    except Exception:
        return None


def standardize_diawt_value(value):
    """Standardize diamond weight format (e.g., '0.5ct tw')"""
    if not value:
        return None

    value = str(value).strip().lower()

    # Normalize slashes and spacing
    value = re.sub(r'\s*/\s*', '/', value)
    value = re.sub(r'\s+', ' ', value)

    # Detect if 'tw' (or variants) exist
    has_tw = any(tw in value for tw in [' tw', 'tw', 't.w.', 'ctw'])

    # Extract the number portion (e.g., 0.5, 3/4, 1-1/2)
    num_match = re.search(r'(\d+-\d+/\d+|\d+/\d+|\d*\.\d+|\d+)', value)
    if not num_match:
        return None

    num_part = num_match.group(1)
    return f"{num_part}ct tw" if has_tw else f"{num_part}ct"



def process_row(row):
    """Process a single row tuple/list to extract Kt and TotalDiaWt values from product name"""
    # Normalize row to length 9 with None padding
    normalized_row = list(row) + [None] * (9 - len(row)) if len(row) < 9 else list(row)

    product_name = normalized_row[3]  # ProductName is at index 3

    extracted_kt = extract_kt_value(product_name)
    extracted_diawt = extract_diawt_value(product_name)

    normalized_row[5] = extracted_kt if extracted_kt else 'NA'     # Index 5 for Kt
    normalized_row[7] = extracted_diawt if extracted_diawt else 'NA'  # Index 7 for TotalDiaWt

    return tuple(normalized_row)