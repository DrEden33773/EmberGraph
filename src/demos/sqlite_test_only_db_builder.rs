use hashbrown::HashMap;
use itertools::Itertools;

use crate::{
  schemas::{DataEdge, DataVertex},
  storage::{SqliteStorageAdapter, TestOnlyStorageAdapter, WritableStorageAdapter},
};

pub(super) struct BI6Builder;

impl BI6Builder {
  pub(super) async fn build() {
    let posts = (1..=3)
      .map(|i| DataVertex {
        vid: format!("post{i}"),
        label: "Post".to_string(),
        attrs: Default::default(),
      })
      .collect_vec();
    let tags = [DataVertex {
      vid: "tag1".to_string(),
      label: "Tag".to_string(),
      attrs: HashMap::from([("name".to_string(), "The_Mouse_and_the_Mask".into())]),
    }];
    let persons = (1..=4)
      .map(|i| DataVertex {
        vid: format!("person{i}"),
        label: "Person".to_string(),
        attrs: Default::default(),
      })
      .collect_vec();

    let has_tag = [DataEdge {
      eid: "1".to_string(),
      src_vid: "post1".to_string(),
      dst_vid: "tag1".to_string(),
      label: "hasTag".to_string(),
      attrs: Default::default(),
    }];
    let has_creator = [
      DataEdge {
        eid: "2".to_string(),
        src_vid: "post1".to_string(),
        dst_vid: "person1".to_string(),
        label: "hasCreator".to_string(),
        attrs: Default::default(),
      },
      DataEdge {
        eid: "4".to_string(),
        src_vid: "post2".to_string(),
        dst_vid: "person2".to_string(),
        label: "hasCreator".to_string(),
        attrs: Default::default(),
      },
      DataEdge {
        eid: "5".to_string(),
        src_vid: "post3".to_string(),
        dst_vid: "person2".to_string(),
        label: "hasCreator".to_string(),
        attrs: Default::default(),
      },
    ];
    let likes = [
      DataEdge {
        eid: "3".to_string(),
        src_vid: "person2".to_string(),
        dst_vid: "post1".to_string(),
        label: "likes".to_string(),
        attrs: Default::default(),
      },
      DataEdge {
        eid: "6".to_string(),
        src_vid: "person3".to_string(),
        dst_vid: "post2".to_string(),
        label: "likes".to_string(),
        attrs: Default::default(),
      },
      DataEdge {
        eid: "7".to_string(),
        src_vid: "person4".to_string(),
        dst_vid: "post3".to_string(),
        label: "likes".to_string(),
        attrs: Default::default(),
      },
    ];

    let storage = SqliteStorageAdapter::async_init_test_only().await;

    for v in posts.into_iter().chain(tags).chain(persons) {
      storage.add_v(v).await.expect("❌  Failed to add vertex");
    }

    for e in has_tag.into_iter().chain(has_creator).chain(likes) {
      storage.add_e(e).await.expect("❌  Failed to add edge");
    }
  }
}
