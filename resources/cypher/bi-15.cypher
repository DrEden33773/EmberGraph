PROFILE
MATCH
    (personA: Person)-[a: knows]->(personB: Person),
    (c1: Comment)-[b: hasCreator]->(personA),
    (c2: Comment)-[c: hasCreator]->(personB),
    (c1)-[d: replyOf]->(c2),
    (c2)-[e: replyOf]->(post: Post),
    (post)<-[f: containerOf]-(forum: Forum)
WHERE
    forum.creationDate >= 1282431075745
RETURN
    personA,
    personB,
    c1,
    c2,
    post,
    forum,
    a,
    b,
    c,
    d,
    e,
    f