use datafrog::Iteration;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
struct Person {
    name: &'static str,
    age: u16,
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
enum Value<'a> {
    Person(&'a Person),
    String(&'a str),
    Int(u16),
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
struct Tup<'a> {
    tail: Value<'a>,
    desc: &'static str,
    head: Value<'a>,
}

fn do_match<'a>(triples: &[Tup<'a>]) -> Vec<(Value<'a>, Value<'a>, Value<'a>)> {
    let mut iteration = Iteration::new();

    let has_name = iteration.variable();
    let has_parent = iteration.variable();

    // has_name(a, name) <- tuple(a, ":name", name)
    has_name.extend(
        triples
            .iter()
            .filter_map(|t| {
                if t.desc == ":name" {
                    Some((t.tail, t.head))
                } else {
                    None
                }
            })
    );

    // has_parent(a, p) <- tuple(a, ":parent", p)
    has_parent.extend(
        triples
            .iter()
            .filter_map(|t| {
                if t.desc == ":parent" {
                    Some((t.tail, t.head))
                } else {
                    None
                }
            })
    );

    let query_1 = iteration.variable();
    let query_2 = iteration.variable();
    let query_3 = iteration.variable();

    while iteration.changed() {
        // query(a, p, name) <- has_name(a, name), has_parent(a, p), has_name(p, name)
        // aka.

        // query_1(p, [a, a_name]) <- has_name(a, a_name), has_parent(a, p)
        query_1.from_join(&has_name, &has_parent, |&a, &a_name, &p| (p, (a, a_name)));

        // query_2(a, [p, a_name, p_name]) <- query1(p, [a, a_name]), has_name(p, p_name)
        query_2.from_join(&query_1, &has_name, |&p, &(a, a_name), &p_name| {
            (a, (p, a_name, p_name))
        });

        // query_3(a, p, name) <- query2(a, [p, name, name])
        query_3.extend(
            query_2
                .recent
                .borrow()
                .elements
                .iter()
                .filter_map(|&(a, (p, a_name, p_name))| {
                    if a_name == p_name {
                        Some((a, p, a_name))
                    } else {
                        None
                    }
                })
        );
    }

    query_3.complete().elements
}

fn main() {
    use rand::Rng;

    // Generate test data.
    let names = ["Lisa", "Ming", "Sriram", "Ivan"];
    let mut people = Vec::new();
    let mut rng = rand::thread_rng();

    for _ in 0..10_000 {
        people.push(Person {
            name: names[rng.gen_range(0, names.len())],
            age: rng.gen_range(1, 99),
        });
    }

    // Create matching tuples.
    let mut triples = Vec::new();
    let possible_parents_by_age = {
        let mut vec = Vec::with_capacity(100);

        for age in 1..100 {
            vec.push(people.iter().filter(|parent| parent.age >= age + 20 && parent.age <= age + 35).collect::<Vec<_>>());
        }

        vec
    };

    for person in &people {
        triples.push(Tup { tail: Value::Person(person), desc: ":name", head: Value::String(person.name) });
        triples.push(Tup { tail: Value::Person(person), desc: ":age", head: Value::Int(person.age) });

        // Find two random parents to assign.
        let possible_parents = &possible_parents_by_age[person.age as usize];

        if possible_parents.len() > 1 {
            let parent_1 = possible_parents[rng.gen_range(0, possible_parents.len() - 1)];
            let parent_2 = possible_parents[rng.gen_range(0, possible_parents.len() - 1)];

            triples.push(Tup { tail: Value::Person(person), desc: ":parent", head: Value::Person(parent_1) });
            triples.push(Tup { tail: Value::Person(person), desc: ":parent", head: Value::Person(parent_2) });
        }
    }

    // Query (a :name name) (a :parent p) (p :name name)
    let start_ts = std::time::SystemTime::now();
    let matches = do_match(&triples);
    let elapsed = start_ts.elapsed().unwrap();

    println!("matches: {:?}", matches.len());
    println!("elapsed: {}ms", elapsed.as_millis());
}
