INSERT INTO vendedores (nome, email) VALUES
  ("Ana Souza", "ana.souza@ciclic.com.br"),
  ("Bruno Lima", "bruno.lima@ciclic.com.br"),
  ("Carla Dias", "carla.dias@ciclic.com.br");

INSERT INTO cupons (codigo, vendedor_id) VALUES
  ("ANA10", 1),
  ("BRUNO10", 2),
  ("CARLA10", 3);

INSERT INTO produtos (nome, canal) VALUES
  ("Seguro Viagem Internacional", "seguro_viagem"),
  ("Seguro Celular Basico", "seguro_celular"),
  ("Seguro Residencial Completo", "seguro_residencial"),
  ("Seguro Saude Individual", "seguro_saude");

INSERT INTO vendas (cupom_id, produto_id, valor, status) VALUES
  (1, 1, 189.90, "confirmada"),
  (2, 2, 49.90, "confirmada"),
  (3, 4, 320.00, "pendente");
