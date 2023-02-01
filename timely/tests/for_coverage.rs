use timely::dataflow::operators::*;

#[test]
fn for_coverage() {
    timely::example(|scope| {
        (0..10)
            .to_stream(scope)
            .map(|v| v * 2)
            .inspect(|x| println!("seen: {:?}", x));
    });
}
