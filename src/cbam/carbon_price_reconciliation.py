class CarbonPriceReconciliation:
    def reconcile(self, paid_price, eu_price):
        return max(eu_price - paid_price, 0)
