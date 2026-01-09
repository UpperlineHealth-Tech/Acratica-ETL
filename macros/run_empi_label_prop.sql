{% macro run_empi_label_prop(edges_rel, nodes_rel, target_rel) %}
EXECUTE IMMEDIATE $$
DECLARE
  changed NUMBER := 1;
BEGIN

  -- Seed labels: label = self
  CREATE OR REPLACE TEMP TABLE seed_labels AS
  SELECT id AS label, id AS node
  FROM {{ nodes_rel }};

  -- Adjacency: both directions + self
  CREATE OR REPLACE TEMP TABLE det_adjacency AS
  SELECT l_id AS src, r_id AS dst FROM {{ edges_rel }}
  UNION ALL
  SELECT r_id AS src, l_id AS dst FROM {{ edges_rel }}
  UNION ALL
  SELECT id AS src, id AS dst FROM {{ nodes_rel }};

  -- Iterate until labels converge
  WHILE (changed > 0) DO

    CREATE OR REPLACE TEMP TABLE next_labels AS
    SELECT
      a.dst,
      MIN(l.label) AS new_label
    FROM det_adjacency a
    JOIN seed_labels  l
      ON l.node = a.src
    GROUP BY a.dst;

    SELECT COUNT(*) INTO :changed
    FROM next_labels n
    JOIN seed_labels l
      ON n.dst = l.node
    WHERE n.new_label <> l.label;

    MERGE INTO seed_labels l
    USING next_labels n
      ON l.node = n.dst
    WHEN MATCHED AND l.label <> n.new_label
      THEN UPDATE SET l.label = n.new_label;
  END WHILE;

  -- Persist crosswalk: node = source_id, label = cluster_id
  CREATE OR REPLACE TABLE {{ target_rel }} AS
  SELECT node AS source_id, label AS cluster_id
  FROM seed_labels;

END;
$$;
{% endmacro %}
