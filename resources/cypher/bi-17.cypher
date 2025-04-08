PROFILE
MATCH
    (tag: Tag)<-[a: hasTag]-(message1: Comment),
    (tag)<-[b: hasTag]-(message2: Comment),
    (tag)<-[c: hasTag]-(comment: Comment),
    (person1: Person)<-[d: hasCreator]-(message1),
    (message1)-[e: replyOf]->(post1: Post),
    (post1)<-[f: containerOf]-(forum1: Forum),
    (forum1)-[g: hasMember]->(person2),
    (forum1)-[h: hasMember]->(person3),
    (person2)<-[i: hasCreator]-(comment),
    (comment)-[j: replyOf]->(message2),
    (person3)<-[k: hasCreator]-(message2),
    (message2)-[l: replyOf]->(post2: Post),
    (post2)<-[m: containerOf]-(forum2: Forum)
WHERE
    tag.name = 'Leonardo_DiCaprio'
    AND message2.creationDate < 1289070384400
    AND forum1.id = 206158431133
    AND forum2.id <> 206158431133
RETURN
    tag,
    message1,
    message2,
    comment,
    person1,
    person2,
    person3,
    post1,
    post2,
    forum1,
    forum2,
    a,
    b,
    c,
    d,
    e,
    f,
    g,
    h,
    i,
    j,
    k,
    l,
    m