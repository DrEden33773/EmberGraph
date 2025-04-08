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
    (post2)<-[m: containerOf]-(forum2: Forum),
    (forum2)-[n: hasMember]->(person1)
WHERE
    tag.name = 'Zine_El_Abidine_Ben_Ali'
    AND message2.creationDate < 1288759005460
ORDER BY
    tag.name DESC,
    message2.creationDate ASC
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
    m,
    n