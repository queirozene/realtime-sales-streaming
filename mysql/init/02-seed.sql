INSERT INTO vendedores (nome, email) VALUES
  ("Ana Souza", "ana.souza@example.com"),
  ("Bruno Lima", "bruno.lima@example.com"),
  ("Carla Dias", "carla.dias@example.com");

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
