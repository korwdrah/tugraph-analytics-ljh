CREATE TABLE hw_person (
    nickname varchar,
    p_id bigint
) WITH (
    type = 'file',
    geaflow.dsl.window.size = -1, -- 读取建表的点和边文件
    geaflow.dsl.file.path = 'resource:///data/grapth_hw_vertex.txt'
);

CREATE TABLE hw_friend (
    src_id bigint,
    target_id bigint
) WITH (
    type = 'file',
    geaflow.dsl.window.size = -1,
    geaflow.dsl.file.path = 'resource:///data/grapth_hw_edges.txt'
);

CREATE GRAPH mutualConnection (
    Vertex person USING hw_person WITH ID(p_id),
    Edge friends USING hw_friend WITH ID(src_id, target_id)
) WITH (
    storeType = 'memory',
    shardCount = 2
);