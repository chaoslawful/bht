<?php

test_conn();
basic();
benchmark(300);
mul_get();
mul_set();
mul_del();
mul_benchmark(300,16);

function test_conn()
{
	$short_conn=array();
	$long_conn=array();
	for($i=0;$i<20;++$i) {
		list($err,$handle)=bht_open("test");
		assert($err === false);
		assert($handle !== null);
		$short_conn[]=$handle;
	}
	for($i=0;$i<40;++$i) {
		list($err,$handle)=bht_popen("test");
		assert($err === false);
		assert($handle !== null);
		$long_conn[]=$handle;
	}

	foreach($short_conn as $handle) {
		$err=bht_close($handle);
		assert($err === false);
	}
	foreach($long_conn as $handle) {
		$err=bht_close($handle);
		assert($err === false);
	}
}

function basic()
{
	list($err,$handle)=bht_open("test","localhost",9090);
	assert($err === false);
	assert($handle !== null);
	
	$err = bht_set($handle, "hello", "world");
	assert($err === false);

	list($err, $val)=bht_get($handle, "hello");
	assert($err === false);
	assert($val !== null);
	echo "bht_get value: ", $val, "\n";

	$err=bht_del($handle, "hello");
	assert($err === false);

	list($err,$val)=bht_get($handle,"hello");
	assert($err === false);
	assert($val === null);
	
	$err=bht_close($handle);
	assert($err === false);
}

function benchmark($ntimes)
{
	list($err,$handle)=bht_open("test");

	$begin=microtime(true);
	for($i=0;$i<$ntimes;++$i) {
		assert(bht_set($handle,"hello",$i,"world") === false);
	}
	$end=microtime(true);
	$total=$end-$begin;
	echo "bht_set: Elaps: ", $total, "s, per op: ", $total/$ntimes, " s, QPS: ", $ntimes/$total, " 1/s\n";

	$begin=microtime(true);
	for($i=0;$i<$ntimes;++$i) {
		list($err,$val)=bht_get($handle,"hello",$i);
		assert($err === false);
		assert($val === "world");
	}
	$end=microtime(true);
	$total=$end-$begin;
	echo "bht_get: Elaps: ", $total, "s, per op: ", $total/$ntimes, " s, QPS: ", $ntimes/$total, " 1/s\n";

	$begin=microtime(true);
	for($i=0;$i<$ntimes;++$i) {
		assert(bht_del($handle,"hello",$i) === false);
	}
	$end=microtime(true);
	$total=$end-$begin;
	echo "bht_del: Elaps: ", $total, "s, per op: ", $total/$ntimes, " s, QPS: ", $ntimes/$total, " 1/s\n";

	list($err,$val)=bht_get($handle,"hello",$i);
	assert($err === false);
	assert($val === null);

	bht_close($handle);
}

function mul_get()
{
	list($err,$h)=bht_open("test");
	assert($err === false);
	assert($h !== null);

	$ks=array("hello",array("google","yahoo"),"foo");
	$vs=array("world","nothing","bar");

	for($i=0;$i<count($ks);++$i) {
		if(is_array($ks[$i])) {
			$err=bht_set($h,$ks[$i][0],$ks[$i][1],$vs[$i]);
			assert($err===false);
		} else {
			$err=bht_set($h,$ks[$i],$vs[$i]);
			assert($err===false);
		}
	}

	$rks=$ks;
	$rks[]="xxx";
	$rks[]=array("y","z");
	$rks[]="zzz";
	list($err,$rvs)=bht_mget($h,$rks);
	assert($err === false);
	assert(count($rvs) === count($rks));

	for($i=0;$i<count($vs);++$i) {
		assert($rvs[$i]===$vs[$i]);
	}
	for($i=count($vs);$i<count($rks);++$i) {
		assert($rvs[$i]===null);
	}

	for($i=0;$i<count($ks);++$i) {
		if(is_array($ks[$i])) {
			$err=bht_del($h,$ks[$i][0],$ks[$i][1]);
			assert($err===false);
		} else {
			$err=bht_del($h,$ks[$i]);
			assert($err===false);
		}
	}

	$err=bht_close($h);
	assert($err === false);
}

function mul_set()
{
	list($err,$h)=bht_open("test");
	assert($err === false);
	assert($h !== null);

	$ks=array("hello",array("google","yahoo"),"foo");
	$vs=array("world","nothing","bar");
	$err=bht_mset($h, $ks, $vs);
	assert($err===false);

	for($i=0;$i<count($ks);++$i) {
		if(is_array($ks[$i])) {
			$err=bht_del($h,$ks[$i][0],$ks[$i][1]);
			assert($err===false);
		} else {
			$err=bht_del($h,$ks[$i]);
			assert($err===false);
		}
	}

	$err=bht_close($h);
	assert($err === false);
}

function mul_del()
{
	list($err,$h)=bht_open("test");
	assert($err === false);
	assert($h !== null);

	$ks=array("hello",array("google","yahoo"),"foo");
	$vs=array("world","nothing","bar");

	for($i=0;$i<count($ks);++$i) {
		if(is_array($ks[$i])) {
			$err=bht_set($h,$ks[$i][0],$ks[$i][1],$vs[$i]);
			assert($err===false);
		} else {
			$err=bht_set($h,$ks[$i],$vs[$i]);
			assert($err===false);
		}
	}

	$err=bht_mdel($h,$ks);
	assert($err===false);

	for($i=0;$i<count($ks);++$i) {
		if(is_array($ks[$i])) {
			$err=bht_del($h,$ks[$i][0],$ks[$i][1]);
			assert($err===false);
		} else {
			$err=bht_del($h,$ks[$i]);
			assert($err===false);
		}
	}

	$err=bht_close($h);
	assert($err === false);
}

function mul_benchmark($ntimes,$nkey)
{
	$keys=array();
	for($i=0;$i<$nkey;++$i) {
		$keys[]=array("key$i","skey$i");
	}
	$vals=array();
	for($i=0;$i<$nkey;++$i) {
		$vals[]="hello,world$i";
	}

	list($err,$handle)=bht_open("test");

	$begin=microtime(true);
	for($i=0;$i<$ntimes;++$i) {
		assert(bht_mset($handle,$keys,$vals) === false);
	}
	$end=microtime(true);
	$total=$end-$begin;
	echo "bht_mset($nkey keys): Elaps: ", $total, "s, per op: ", $total/$ntimes, " s, QPS: ", $ntimes/$total, " 1/s\n";

	$begin=microtime(true);
	for($i=0;$i<$ntimes;++$i) {
		list($err,$vs)=bht_mget($handle,$keys);
		assert($err === false);
		for($j=0;$j<$nkey;++$j) {
			assert($vs[$j] === $vals[$j]);
		}
	}
	$end=microtime(true);
	$total=$end-$begin;
	echo "bht_mget($nkey keys): Elaps: ", $total, "s, per op: ", $total/$ntimes, " s, QPS: ", $ntimes/$total, " 1/s\n";

	$begin=microtime(true);
	for($i=0;$i<$ntimes;++$i) {
		assert(bht_mdel($handle,$keys) === false);
	}
	$end=microtime(true);
	$total=$end-$begin;
	echo "bht_mdel($nkey keys): Elaps: ", $total, "s, per op: ", $total/$ntimes, " s, QPS: ", $ntimes/$total, " 1/s\n";

	list($err,$vs)=bht_mget($handle,$keys);
	assert($err === false);
	for($j=0;$j<$nkey;++$j) {
		assert($vs[$j] === null);
	}

	bht_close($handle);
}

?>
