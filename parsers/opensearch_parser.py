import pandas as pd
from typing import List, Optional
from pathlib import Path
from models.log_entry import LogEntry

class OpenSearchParser:
    """
    Lê arquivos CSV exportados do OpenSearch e converte em objetos LogEntry.
    
    Analogia: É como um scanner que lê documentos em papel e 
    organiza tudo em pastas digitais etiquetadas.
    """
    
    def __init__(self, csv_path: str):
        """
        Inicializa o parser com o caminho do arquivo CSV.
        
        Args:
            csv_path: Caminho completo do arquivo CSV
        """
        self.csv_path = Path(csv_path)
        self.df: Optional[pd.DataFrame] = None
        self.logs: List[LogEntry] = []
    
    def load(self) -> 'OpenSearchParser':
        """
        Carrega o arquivo CSV em memória.
        
        Returns:
            Self (permite encadeamento: parser.load().parse())
        """
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {self.csv_path}")
        
        print(f"📂 Carregando CSV: {self.csv_path.name}")
        
        # Pandas lê o CSV e cria um DataFrame (tabela)
        self.df = pd.read_csv(self.csv_path)
        
        print(f"✅ {len(self.df)} registros carregados")
        
        return self
    
    def parse(self) -> List[LogEntry]:
        """
        Converte cada linha do DataFrame em um objeto LogEntry.
        
        Returns:
            Lista de objetos LogEntry
        """
        if self.df is None:
            raise ValueError("CSV não foi carregado. Execute .load() primeiro.")
        
        print(f"🔄 Convertendo logs em objetos...")
        
        self.logs = []
        
        # Itera sobre cada linha do DataFrame
        for index, row in self.df.iterrows():
            # Converte a linha (Series do pandas) em dicionário
            raw_data = row.to_dict()
            
            # Cria um objeto LogEntry
            log_entry = LogEntry(raw_data)
            
            # Adiciona na lista
            self.logs.append(log_entry)
        
        print(f"✅ {len(self.logs)} logs processados")
        
        return self.logs
    
    def get_by_correlation_id(self, correlation_id: str) -> List[LogEntry]:
        """
        Filtra logs por correlation_id.
        
        Analogia: É como procurar todas as páginas de um processo
        usando o número do protocolo.
        
        Args:
            correlation_id: ID da correlação para filtrar
            
        Returns:
            Lista de logs com aquele correlation_id
        """
        return [log for log in self.logs if log.correlation_id == correlation_id]
    
    def get_by_date(self, date_str: str) -> List[LogEntry]:
        """
        Filtra logs por data (formato: 20/01/2026).
        
        Args:
            date_str: Data no formato DD/MM/YYYY
            
        Returns:
            Lista de logs daquela data
        """
        # Converte 20/01/2026 para "Jan 20, 2026"
        from datetime import datetime
        
        try:
            # Parse da data brasileira
            date_obj = datetime.strptime(date_str, "%d/%m/%Y")
            
            # Formato que vem no OpenSearch: "Jan 20, 2026"
            opensearch_format = date_obj.strftime("%b %d, %Y")
            
            # Filtra logs que contenham essa data no timestamp
            return [log for log in self.logs 
                    if opensearch_format in log.timestamp]
        
        except ValueError:
            print(f"❌ Data inválida: {date_str}. Use o formato DD/MM/YYYY")
            return []
    
    def get_by_cpf(self, cpf: str) -> List[LogEntry]:
        """
        Filtra logs que contenham um CPF específico no input.
        
        Args:
            cpf: CPF a buscar (com ou sem formatação)
            
        Returns:
            Lista de logs que mencionam esse CPF
        """
        # Remove formatação do CPF
        cpf_limpo = cpf.replace(".", "").replace("-", "")
        
        return [log for log in self.logs 
                if cpf_limpo in str(log.input)]
    
    def get_by_phone(self, phone: str) -> List[LogEntry]:
        """
        Filtra logs que contenham um telefone específico no input ou metadata.
        
        Args:
            phone: Telefone a buscar
            
        Returns:
            Lista de logs que mencionam esse telefone
        """
        return [log for log in self.logs 
                if phone in str(log.input) or phone in str(log.metadata)]
    
    def get_summary(self) -> dict:
        """
        Gera um resumo estatístico dos logs carregados.
        
        Returns:
            Dicionário com estatísticas
        """
        if not self.logs:
            return {}
        
        total = len(self.logs)
        success_count = sum(1 for log in self.logs if log.success)
        failed_count = total - success_count
        
        # Conta endpoints mais usados
        endpoints = {}
        for log in self.logs:
            endpoint = log.endpoint_name
            endpoints[endpoint] = endpoints.get(endpoint, 0) + 1
        
        # Ordena por frequência
        top_endpoints = sorted(endpoints.items(), 
                              key=lambda x: x[1], 
                              reverse=True)[:5]
        
        return {
            'total': total,
            'success': success_count,
            'failed': failed_count,
            'success_rate': f"{(success_count/total)*100:.1f}%",
            'top_endpoints': top_endpoints
        }