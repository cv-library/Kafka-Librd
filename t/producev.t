use strict;
use warnings;
use Test::More;

my $message_max_bytes = 1000;

BEGIN { use_ok "Kafka::Librd" }
my $krd = Kafka::Librd->new(
    Kafka::Librd::RD_KAFKA_PRODUCER,
    {
        "message.max.bytes" => $message_max_bytes,
    }
);

{
    eval { $krd->producev };
    like $@, qr[^Usage: Kafka::Librd::producev\(rdk, params\)],
        "missing params";
}

{
    eval { $krd->producev( {} ) };
    like $@, qr[^topic must be defined], "missing topic";
}

{
    eval { $krd->producev( { topic => undef, } ) };
    like $@, qr[^topic must be defined], "undef topic";
}

{
    eval {
        $krd->producev({
            topic     => "foo",
            partition => "not a number",
        });
    };
    like $@, qr[^partition must be a number], "wrong type for partition";
}

{
    eval {
        $krd->producev({
            topic    => "foo",
            msgflags => "not a number",
        });
    };
    like $@, qr[^msgflags must be a number], "wrong type for msgflags";
}

{
    eval {
        $krd->producev({
            topic   => "foo",
            headers => "not a hash reference",
        });
    };
    like $@, qr[^headers must be a hash reference], "wrong type for headers";
}

{
    my $err = $krd->producev({
        topic     => "foo",
        value     => "a" x ($message_max_bytes + 1),
    });
    is $err, Kafka::Librd::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE,
        "message size too large";
}

{
    is $krd->producev({
        topic     => "foo",
        partition => 1,
        msgflags  => 0,
        key       => "key",
        value     => "value",
        headers   => { foo => 1, },
    }), Kafka::Librd::RD_KAFKA_RESP_ERR_NO_ERROR , "happy path";
}

done_testing;
