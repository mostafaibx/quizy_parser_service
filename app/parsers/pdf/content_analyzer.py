"""
Content Analyzer Module
Extracts key topics, summaries, and question areas from document content
"""
from typing import Dict, Any, List
import re
from collections import Counter
import logging

logger = logging.getLogger(__name__)


def extract_key_topics(text: str, language: str = "en", max_topics: int = 15) -> List[str]:
    """
    Extract key topics from text using frequency analysis and NLP heuristics
    Supports English, Arabic, German, French, Spanish, and Italian

    Args:
        text: Document text
        language: Language code (en, ar, de, fr, es, it)
        max_topics: Maximum number of topics to extract

    Returns:
        List of key topics
    """
    if not text:
        return []

    # Clean text - preserve language-specific characters
    if language == "ar":
        text_clean = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', text.lower())
    elif language == "de":
        text_clean = re.sub(r'[^\w\säöüÄÖÜß]', ' ', text.lower())
    elif language in ["fr", "es", "it"]:
        # Preserve accented characters for Romance languages
        text_clean = re.sub(r'[^\w\sàâäæçéèêëïîôùûüÿœÀÂÄÆÇÉÈÊËÏÎÔÙÛÜŸŒáéíóúñÁÉÍÓÚÑàèéìíîòóùúÀÈÉÌÍÎÒÓÙÚ]', ' ', text.lower())
    else:
        text_clean = re.sub(r'[^\w\s]', ' ', text.lower())

    words = text_clean.split()

    # Language-specific stopwords
    stopwords = _get_stopwords(language)

    # Extract noun phrases (capitalized sequences)
    noun_phrases = _extract_noun_phrases(text, language)

    # Filter meaningful words
    meaningful_words = [
        w for w in words
        if w not in stopwords and len(w) > 3 and not w.isdigit()
    ]

    # Count frequencies
    word_freq = Counter(meaningful_words)
    phrase_freq = Counter(noun_phrases)

    # Combine topics
    topics = []

    # Add top phrases (multi-word concepts)
    for phrase, count in phrase_freq.most_common(max_topics // 2):
        if count > 1 and len(phrase.split()) <= 4:
            topics.append(phrase)

    # Add top single words not already in phrases
    topics_lower = ' '.join(topics).lower()
    for word, count in word_freq.most_common(max_topics):
        if count > 2 and word not in topics_lower:
            topics.append(word)
            if len(topics) >= max_topics:
                break

    return topics[:max_topics]


def _get_stopwords(language: str) -> set:
    """Get language-specific stopwords"""
    if language == "ar":
        # Arabic stopwords
        return {
            'في', 'من', 'إلى', 'على', 'عن', 'مع', 'بعد', 'قبل', 'عند', 'لكن', 'أو', 'و',
            'هذا', 'هذه', 'ذلك', 'تلك', 'التي', 'الذي', 'اللتان', 'اللذان', 'اللواتي', 'الذين',
            'هو', 'هي', 'نحن', 'أنت', 'أنتم', 'هم', 'هن', 'أنا',
            'كان', 'كانت', 'كانوا', 'يكون', 'تكون', 'يكونوا', 'أصبح', 'أصبحت', 'صار', 'صارت',
            'لم', 'لن', 'لا', 'ما', 'متى', 'أين', 'كيف', 'لماذا', 'ماذا', 'أي',
            'كل', 'بعض', 'جميع', 'كثير', 'قليل', 'أكثر', 'أقل', 'جدا', 'أيضا', 'كذلك',
            'ثم', 'أم', 'بل', 'حتى', 'إذا', 'إذ', 'منذ', 'بينما', 'لأن', 'كي', 'لكي'
        }
    elif language == "de":
        # German stopwords
        return {
            'der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine', 'einen', 'einem', 'eines',
            'und', 'oder', 'aber', 'doch', 'sondern', 'denn', 'weil', 'dass', 'ob', 'wenn', 'als',
            'ist', 'sind', 'war', 'waren', 'bin', 'bist', 'hat', 'haben', 'hatte', 'hatten',
            'wird', 'werden', 'wurde', 'wurden', 'kann', 'können', 'muss', 'müssen', 'soll', 'sollen',
            'ich', 'du', 'er', 'sie', 'es', 'wir', 'ihr', 'Sie', 'mich', 'dich', 'sich',
            'in', 'an', 'auf', 'aus', 'bei', 'mit', 'nach', 'von', 'zu', 'vor', 'über', 'unter',
            'diese', 'dieser', 'dieses', 'jene', 'jener', 'jenes', 'welche', 'welcher', 'welches',
            'hier', 'da', 'dort', 'jetzt', 'nun', 'dann', 'heute', 'gestern', 'morgen',
            'sehr', 'viel', 'mehr', 'weniger', 'alle', 'einige', 'manche', 'jeder', 'kein', 'nicht',
            'was', 'wer', 'wie', 'wo', 'wann', 'warum', 'woher', 'wohin', 'womit', 'wofür'
        }
    elif language == "fr":
        # French stopwords
        return {
            'le', 'la', 'les', 'un', 'une', 'des', 'du', 'de', 'au', 'aux',
            'et', 'ou', 'mais', 'donc', 'or', 'ni', 'car',
            'est', 'sont', 'était', 'étaient', 'sera', 'seront', 'a', 'ont', 'avait', 'avaient',
            'je', 'tu', 'il', 'elle', 'nous', 'vous', 'ils', 'elles', 'me', 'te', 'se',
            'dans', 'sur', 'sous', 'avec', 'sans', 'pour', 'par', 'en', 'à',
            'ce', 'cet', 'cette', 'ces', 'quel', 'quelle', 'quels', 'quelles',
            'qui', 'que', 'quoi', 'où', 'quand', 'comment', 'pourquoi',
            'pas', 'plus', 'moins', 'très', 'tout', 'tous', 'toute', 'toutes', 'même',
            'ici', 'là', 'maintenant', 'alors', 'encore', 'aussi', 'déjà'
        }
    elif language == "es":
        # Spanish stopwords
        return {
            'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas', 'del', 'al',
            'y', 'o', 'pero', 'sino', 'porque', 'pues',
            'es', 'son', 'era', 'eran', 'será', 'serán', 'ha', 'han', 'había', 'habían',
            'yo', 'tú', 'él', 'ella', 'nosotros', 'vosotros', 'ellos', 'ellas', 'me', 'te', 'se',
            'en', 'sobre', 'bajo', 'con', 'sin', 'para', 'por', 'de', 'a',
            'este', 'esta', 'estos', 'estas', 'ese', 'esa', 'esos', 'esas',
            'que', 'qué', 'quién', 'dónde', 'cuándo', 'cómo',
            'no', 'más', 'menos', 'muy', 'todo', 'todos', 'toda', 'todas', 'mismo',
            'aquí', 'ahí', 'ahora', 'entonces', 'también', 'ya'
        }
    elif language == "it":
        # Italian stopwords
        return {
            'il', 'lo', 'la', 'i', 'gli', 'le', 'un', 'uno', 'una', 'del', 'al',
            'e', 'o', 'ma', 'però', 'perché', 'quindi',
            'è', 'sono', 'era', 'erano', 'sarà', 'saranno', 'ha', 'hanno', 'aveva', 'avevano',
            'io', 'tu', 'lui', 'lei', 'noi', 'voi', 'loro', 'mi', 'ti', 'si',
            'in', 'su', 'sotto', 'con', 'senza', 'per', 'da', 'di', 'a',
            'questo', 'questa', 'questi', 'queste', 'quello', 'quella', 'quelli', 'quelle',
            'che', 'chi', 'dove', 'quando', 'come',
            'non', 'più', 'meno', 'molto', 'tutto', 'tutti', 'tutta', 'tutte', 'stesso',
            'qui', 'là', 'ora', 'adesso', 'allora', 'anche', 'già'
        }
    else:
        # English stopwords (default)
        return {
            'the', 'is', 'at', 'which', 'on', 'a', 'an', 'and', 'or', 'but', 'in',
            'with', 'to', 'for', 'of', 'as', 'from', 'by', 'that', 'this', 'these',
            'those', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'should', 'could', 'may', 'might', 'can', 'it', 'its',
            'are', 'was', 'were', 'am', 'such', 'very', 'too', 'also', 'about', 'after',
            'all', 'any', 'because', 'before', 'between', 'both', 'during', 'each',
            'few', 'more', 'most', 'other', 'some', 'than', 'then', 'there',
            'they', 'through', 'under', 'up', 'who', 'why', 'you', 'your', 'how',
            'what', 'when', 'where', 'while', 'into', 'over', 'same', 'not', 'only',
            'own', 'so', 'just', 'now', 'here', 'however', 'still', 'even', 'back',
            'well', 'way', 'down', 'out', 'off', 'since', 'around', 'much', 'many'
        }


def _extract_noun_phrases(text: str, language: str) -> List[str]:
    """Extract potential noun phrases from text (multilingual support)"""
    phrases = []

    if language == "ar":
        # Arabic pattern - words starting with capital Arabic letters or important terms
        # Arabic doesn't use capitalization, so look for key term patterns
        arabic_patterns = [
            r'(?:مفهوم|نظرية|قانون|مبدأ|طريقة|أسلوب|منهج)\s+[\u0600-\u06FF]+',  # concept/theory/law + word
            r'[\u0600-\u06FF]+\s+(?:الأول|الثاني|الثالث|الرابع|الخامس)',  # word + first/second/third
            r'(?:علم|دراسة)\s+[\u0600-\u06FF]+',  # science/study of + word
        ]
        for pattern in arabic_patterns:
            found = re.findall(pattern, text)
            phrases.extend(found)

    elif language == "de":
        # German pattern - nouns are capitalized
        pattern = r'\b[A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)*\b'
        phrases = re.findall(pattern, text)
        # Filter out German articles and common words
        filtered_phrases = []
        for phrase in phrases:
            if len(phrase) > 2 and phrase not in ['Der', 'Die', 'Das', 'Ein', 'Eine', 'Diese', 'Dieser']:
                filtered_phrases.append(phrase)
        phrases = filtered_phrases

    else:
        # English/Romance languages pattern - capitalized sequences
        pattern = r'\b[A-ZÀÂÄÆÇÉÈÊËÏÎÔÙÛÜŸŒÁÉÍÓÚÑ][a-zàâäæçéèêëïîôùûüÿœáéíóúñ]+(?:\s+[A-ZÀÂÄÆÇÉÈÊËÏÎÔÙÛÜŸŒÁÉÍÓÚÑ][a-zàâäæçéèêëïîôùûüÿœáéíóúñ]+)*\b'
        found = re.findall(pattern, text)
        # Filter out common non-topic phrases
        phrases = [
            phrase for phrase in found
            if len(phrase) > 2 and phrase not in ['The', 'This', 'That', 'These', 'Those', 'Le', 'La', 'Les', 'El', 'La', 'Los', 'Las', 'Il', 'Lo']
        ]

    return phrases


def generate_summary_points(
    pages: List[Dict[str, Any]],
    key_topics: List[str],
    max_points: int = 10
) -> List[str]:
    """
    Generate summary points from document content

    Args:
        pages: List of page dictionaries with content
        key_topics: Previously extracted key topics
        max_points: Maximum number of summary points

    Returns:
        List of summary points
    """
    summary_points = []

    # Extract important sentences from beginning of document
    for page in pages[:5]:  # First 5 pages
        content = page.get("content", "")
        if not content:
            continue

        sentences = [s.strip() for s in content.split('.') if s.strip()]

        # Add first substantial sentence from each early page
        for sentence in sentences[:3]:  # First 3 sentences per page
            if _is_summary_worthy(sentence, key_topics):
                summary_points.append(sentence + ".")
                if len(summary_points) >= max_points // 2:
                    break

    # Add structural information
    total_tables = sum(len(p.get("elements", {}).get("tables", [])) for p in pages)
    total_images = sum(len(p.get("elements", {}).get("images", [])) for p in pages)
    total_equations = sum(len(p.get("elements", {}).get("equations", [])) for p in pages)

    if total_tables > 0:
        summary_points.append(f"Contains {total_tables} table{'s' if total_tables > 1 else ''} with data")

    if total_images > 0:
        summary_points.append(f"Includes {total_images} figure{'s' if total_images > 1 else ''} and illustration{'s' if total_images > 1 else ''}")

    if total_equations > 0:
        summary_points.append(f"Features {total_equations} mathematical equation{'s' if total_equations > 1 else ''}")

    # Look for conclusions or key findings in later pages
    for page in pages[-3:]:  # Last 3 pages
        content = page.get("content", "")
        if any(keyword in content.lower() for keyword in ['conclusion', 'summary', 'findings', 'results']):
            sentences = [s.strip() for s in content.split('.') if s.strip()]
            for sentence in sentences:
                if _is_conclusion_sentence(sentence):
                    summary_points.append(sentence + ".")
                    if len(summary_points) >= max_points:
                        break

    return summary_points[:max_points]


def _is_summary_worthy(sentence: str, key_topics: List[str]) -> bool:
    """Determine if a sentence is worthy of being in summary"""
    # Check length
    if len(sentence) < 20 or len(sentence) > 200:
        return False

    # Check if contains key topics
    sentence_lower = sentence.lower()
    topic_count = sum(1 for topic in key_topics if topic.lower() in sentence_lower)

    # Check for summary indicators
    summary_indicators = ['important', 'significant', 'main', 'key', 'primary', 'essential']
    has_indicator = any(ind in sentence_lower for ind in summary_indicators)

    return topic_count > 0 or has_indicator


def _is_conclusion_sentence(sentence: str) -> bool:
    """Check if sentence is likely a conclusion or finding"""
    conclusion_patterns = [
        'conclude', 'finding', 'result', 'show', 'demonstrate',
        'indicate', 'suggest', 'prove', 'evidence', 'therefore'
    ]

    sentence_lower = sentence.lower()
    return any(pattern in sentence_lower for pattern in conclusion_patterns) and len(sentence) > 30


def identify_question_areas(text: str) -> List[Dict[str, str]]:
    """
    Identify areas suitable for quiz questions
    
    Args:
        text: Document text content
        
    Returns:
        List of question areas with type and hint
    """
    question_areas = []

    # Look for definitions
    definition_pattern = r'(\w+)\s+is\s+(?:defined as|a|an)\s+([^.]+)'
    for match in re.finditer(definition_pattern, text):
        question_areas.append({
            "type": "definition",
            "term": match.group(1),
            "definition": match.group(2)
        })

    # Look for comparisons
    if 'compared to' in text or 'versus' in text or 'differ' in text:
        question_areas.append({
            "type": "comparison",
            "hint": "Compare and contrast concepts mentioned in the text"
        })

    # Look for processes or steps
    if any(word in text.lower() for word in ['step', 'process', 'procedure', 'method']):
        question_areas.append({
            "type": "process",
            "hint": "Explain the steps or process described"
        })

    return question_areas[:10]  # Limit to 10 areas
