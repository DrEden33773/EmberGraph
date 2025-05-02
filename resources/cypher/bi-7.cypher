MATCH
    (comment: Comment)-[a: replyOf]->(post: Post),
    (post)-[b: hasTag]->(tag: Tag),
    (comment)-[c: hasTag]->(relatedTag: Tag)
WHERE
    tag.name = '50_Cent' AND
    relatedTag.name <> '50_Cent'
RETURN
    comment, post, tag, relatedTag, a, b, c