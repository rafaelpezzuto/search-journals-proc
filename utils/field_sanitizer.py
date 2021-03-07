# coding=utf-8
import unicodedata

from datetime import datetime


# Date composed by more than four digits
DATE_ERROR_LEVEL_SIZE = 1

# Date composed by invalid chars
DATE_ERROR_LEVEL_CHAR = 2

# Year out of interval [1000, 2020]
DATE_ERROR_LEVEL_VALUE = 3

# Empty date
DATE_ERROR_LEVEL_EMPTY = 4

# Author's name composed by invalid char
AUTHOR_ERROR_LEVEL_CHAR = 1

# A initial version of a list of invalid chars (for checking author's fullname)
INVALID_CHARS = {u'@', u'≈', u'≠', u'‼', u'∗', u'¾', u'²', u'}', u'=', u'‰', u'¶', u'±', u'³', u'©', u'¼', u'[', u']',
                 u'#', u'?', u')', '(', u'{', u'0', u'1', u'2', u'3', u'4', u'5', u'6', u'7', u'8', u'9', u'!', u'¡',
                 u'¿', u'«', u'»', u'*', u'/', u'\\', u'&', u'%', u'‖', u'§', u'®', u'¹', u'½'}


def get_date_quality(date):
    """ Identify the consistence (quality) of a date according to the following rules:

    (a) If date has no chars, the method returns DATE_ERROR_LEVEL_EMPTY
    (b) If date has less than four digits, the method returns DATE_ERROR_LEVEL_SIZE
    (c) If date is composed of chars others than digits, the method returns DATE_ERROR_LEVEL_CHAR
    (d) If date represents an year less than 1000 or greater than the current year,  the method returns
    DATE_ERROR_LEVEL_VALUE.

    We are supposing that a citation whose year of publication date is less than 1000, is probably a citation with
    an invalid publication date.

    :param date: date (YYYY-MM-DD)
    :return: an int representing the consistency (quality) of the date
    """
    if not date:
        return DATE_ERROR_LEVEL_EMPTY

    # Get the first four elements of the date
    if len(date) > 4:
        date = date[:4]

    if len(date) < 4:
        return DATE_ERROR_LEVEL_SIZE
    elif not date.isdigit():
        return DATE_ERROR_LEVEL_CHAR
    elif int(date) < 1000 or int(date) > datetime.now().year:
        return DATE_ERROR_LEVEL_VALUE

def get_author_name_quality(text):
    for c in text:
        if c in INVALID_CHARS:
            return AUTHOR_ERROR_LEVEL_CHAR

def remove_period(text):
    if not text:
        return ''
    else:
        while text.endswith('.'):
            text = text[:-1]
        return text


def remove_accents(text):
    if not text:
        return ''
    else:
        cleaned_text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
        if cleaned_text:
            return cleaned_text
        else:
            return ''


def keep_alpha_digit(text):
    cleaned_text = []
    for i in remove_accents(text):
        if i.isalpha() and not i.isdigit():
            cleaned_text.append(i)
    return ''.join(cleaned_text).lower()
