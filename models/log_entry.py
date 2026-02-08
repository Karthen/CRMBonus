import json
from datetime import datetime  # ← Corrigido: era "detetime"
from typing import Dict, Any, Optional

class LogEntry:
    """
    Representa uma única linha do log do OpenSearch.
    
    Analogia: É como uma ficha de atendimento médico.
    Cada paciente (requisição) tem seus dados organizados.
    """
    
    def __init__(self, raw_data: Dict[str, Any]):  
        """
        Inicializa o log a partir dos dados brutos do CSV.
        
        Args:
            raw_data: Dicionário com os dados da linha do CSV
        """
        self.raw_data = raw_data
        self.correlation_id = raw_data.get('_source.correlation_id', '')
        self.timestamp = raw_data.get('_source.timestamp', '')
        self.success = self._parse_bool(raw_data.get('_source.success', 'false'))
        
        # Identificação
        self.customer_id = raw_data.get('_source.entity.id', '') 
        self.endpoint = raw_data.get('_source.current.feature', '') 
        
        # Input e Output
        self.input = self._parse_json(raw_data.get('_source.input', '{}'))
        self.output = self._parse_json(raw_data.get('_source.output', '{}'))

        # Metadata
        self.metadata = self._parse_json(raw_data.get('_source.metadata', '{}'))

        # Mensagens de erro (se tiver)
        self.message = raw_data.get('_source.message', '')
    
    def _parse_json(self, json_string: str) -> Dict[str, Any]: 
        """
        Converte string JSON em dicionário Python.
        
        Analogia: É como traduzir uma receita médica manuscrita
        para um formato digital legível.
        """
        if not json_string or json_string == '[]':
            return {} 
        
        try:
            return json.loads(json_string)
        except json.JSONDecodeError:
            # Se não conseguir parse, retorna vazio
            return {} 
    
    def _parse_bool(self, value: str) -> bool:
        """Converte string 'true'/'false' para booleano Python."""
        return str(value).lower() == 'true'
    
    @property
    def api_version(self) -> str:
        """
        Identifica qual versão da API foi usada.
        
        Exemplo: 
        - 'ApiPdvV2::user_verify' -> 'pdv-v2'
        - 'ApiEcommerceV1::bonus_apply' -> 'ecommerce-v1'
        """
        endpoint = self.endpoint.lower()
        
        # PDV v2
        if 'apipdvv2' in endpoint:
            return 'pdv-v2'
        
        # PDV v1
        elif 'apipdvv1' in endpoint or 'pages::api_' in endpoint:
            return 'pdv-v1'
        
        # Ecommerce v2
        elif 'apiecommerce' in endpoint and 'v2' in endpoint:
            return 'ecommerce-v2'
        
        # Ecommerce v1
        elif 'apiecommerce' in endpoint:
            return 'ecommerce-v1'
        
        else:
            return 'desconhecido'
        
    @property
    def endpoint_name(self) -> str:
        """
        Extrai o nome do endpoitn sem o prefixo
        
        Exemplo: 'ApiPdvV2::user_verify' -> 'user_verify'
        """
        if '::' in self.endpoint:
            return self.endpoint.split('::')[1]
        return self.endpoint
    
    @property
    def duration_ms(self) -> float:
        """
        Calcula a duração total da req em milissigundos;
        """
        checkpoints = self.metadata.get('checkpoints_ms', '{}')
        if not checkpoints:
            return 0.0
        
        #Pega o último checkpoint (maior valor)
        return max(checkpoints.values()) if checkpoints else 0.0
    
    def __repr__(self) -> str:
        """
        Representação legível do objeto para deubg.
        """
        return(f"LogEntry(correlation_id=)'{self.correlation_id}', "
               f"endopoint='{self.endpoint}', success={self.success}")