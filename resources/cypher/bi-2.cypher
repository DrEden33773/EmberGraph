MATCH
    (post: Post)-[a: hasTag]->(tag: Tag),
    (comment: Comment)-[b: hasTag]->(tag),
    (tag)-[c: hasType]->(tagClass: Tagclass)
WHERE
    tag.name = 'Patrick_Vieira'
RETURN
    post, tag, comment, tagClass, a, b, c