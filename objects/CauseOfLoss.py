class CauseOfLoss:

    def __init__(self, cause_name):
        self.cause_name = cause_name
        self.cause_variable = "v_" + cause_name.upper().replace(',', '').replace(" - ", ' ').replace('(', '')\
                .replace(')', '').replace('\\', '_').replace('/', '_').replace('&', '\' || CHR(38) || \'')\
                                        .replace('-', '').replace(' ', '_')[:20]
