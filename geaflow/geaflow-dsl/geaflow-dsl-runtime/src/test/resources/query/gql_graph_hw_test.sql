CREATE TABLE result_tb (
    p_id bigint,
    circle_length bigint
) WITH (
    type = 'file',
    geaflow.dsl.file.path = '${target}'
);

-- 自己创建的图 人物关系
USE GRAPH mutualConnection;

-- 调用算法并将结果处处到指定文件中
INSERT INTO result_tb
CALL single_vertex_circles_detection() YIELD (p_id, circle_length)
RETURN p_id, circle_length;