def round_to_value(number, precision):

    """
    Rounds a number to desired precision - say you want to round to nearest half decimal (0.5, or 0.05, etc).

    Parameters
    ----------
    number : float
        number to be rounded.
    precision : float
        this is the desired rounder - as in if I wanted 1.05, I'd pass (number = 1.03, precision = 0.05).

    Returns
    -------
    rounded : float
        number rounded to desired accuracy, with adjustment for float artifacts.

    """

    rounded = round(number / precision) * precision
    return round(rounded, 6)
