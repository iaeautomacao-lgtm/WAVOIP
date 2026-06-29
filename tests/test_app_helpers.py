import unittest
import sys
import types

dotenv_stub = types.ModuleType("dotenv")
dotenv_stub.load_dotenv = lambda *_args, **_kwargs: False
sys.modules["dotenv"] = dotenv_stub

import app


class PhoneNormalizationTests(unittest.TestCase):
    def test_normalize_phone_handles_plain_punctuation_and_float_suffix(self):
        self.assertEqual(app._normalize_phone("(11) 99999-8888"), "11999998888")
        self.assertEqual(app._normalize_phone("11999998888.0"), "11999998888")

    def test_normalize_phone_handles_spreadsheet_scientific_notation(self):
        self.assertEqual(app._normalize_phone("1.199998888E10"), "11999988880")

    def test_phone_e164_adds_brazil_country_code_when_missing(self):
        self.assertEqual(app._phone_e164("(11) 99999-8888"), "+5511999998888")
        self.assertEqual(app._phone_e164("+55 11 99999-8888"), "+5511999998888")


class DocumentNormalizationTests(unittest.TestCase):
    def test_normalize_cpf_removes_mask_and_zero_pads(self):
        self.assertEqual(app.normalize_cpf("123.456.789-10"), "12345678910")
        self.assertEqual(app.normalize_cpf("123"), "00000000123")


class AgreementParsingTests(unittest.TestCase):
    def test_detecta_acordo_formalizado_by_known_phrases(self):
        self.assertTrue(app._detectar_acordo_formalizado("Combinado entao, vou enviar o boleto."))
        self.assertTrue(app._detectar_acordo_formalizado("Negociacao concluida com sucesso."))
        self.assertFalse(app._detectar_acordo_formalizado("Cliente pediu para retornar depois."))

    def test_extrair_forma_pagamento_prioritizes_boleto_and_cartao(self):
        self.assertEqual(app._extrair_forma_pagamento("Pode mandar o boleto por email"), "Boleto")
        self.assertEqual(app._extrair_forma_pagamento("Vou pagar no cartao"), "Cartão")
        self.assertEqual(app._extrair_forma_pagamento("Pagamento combinado"), "À vista")

    def test_extrair_valor_from_summary(self):
        self.assertEqual(app._extrair_valor("Acordo fechado em R$ 1.234,56 para hoje"), "1.234,56")
        self.assertEqual(app._extrair_valor("Sem valor informado"), "")


class LineTokenTests(unittest.TestCase):
    def test_clean_line_tokens_keeps_only_known_unique_tokens(self):
        valid = app.DEVICE_PRIORITY[0]
        unknown = "token-invalido"

        self.assertEqual(app._clean_line_tokens([valid, valid, unknown, ""]), [valid])
        self.assertEqual(app._clean_line_tokens(f"{valid},{unknown},{valid}"), [valid])


if __name__ == "__main__":
    unittest.main()
