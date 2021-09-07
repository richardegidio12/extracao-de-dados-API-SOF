import pytest
from classes_contratos import RequestApi


class TesteRequestApi:

    def test_get_endpoint_init(self):
        actual = RequestApi("contratos")._get_endpoint_init()
        expected = "https://gatewayapi.prodam.sp.gov.br:443/financas/orcamento/sof/v3.0.1/contratos"
        assert actual == expected


    @pytest.mark.parametrize(
        "tabela, ano, expected",
        [
            ("contratos", 2020, 6041201900026023),
            ("contratos", 2021, 6046202000063670)
        ]

    )
    def test_consult_data(self, tabela, ano, expected):
        actual_ = RequestApi(tabela=tabela)._consult_data(ano=ano)
        actual = actual_["lstContratos"][0]['codProcesso']
        assert actual == expected
