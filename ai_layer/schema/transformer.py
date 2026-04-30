def transaction_to_text(row):
    """
    Converts raw DB row into meaningful NLP text
    """

    account, transaction_date, details, chq, value_date, withdrawal_amt, deposit_amt, balance_amt, source = row[:9]

    return (
        f"On {transaction_date}, transaction '{details}' occurred. "
        f"Withdrawal: {withdrawal_amt or 0}, Deposit: {deposit_amt or 0}. "
        f"Balance updated to {balance_amt}. "
        f"Source system: {source}."
    )