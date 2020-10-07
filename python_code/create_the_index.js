var fs =require("fs");
var FlexSearch = require("flexsearch");

var index = new FlexSearch({

	encode: "balance",
	tokenize: "forward",
	threshold: 0,
	async: false,
	worker: false,
	cache: false

});

function fileread(filename)
{            
   var contents= fs.readFileSync(filename);
   return JSON.parse(contents);
} 

const request_data = async () => {

	let home_dir=process.env['HOME']
	var data=fileread(home_dir+'/Dropbox/websites/worldwideneuro.com/seminar_data.json');
	
	entries=Object.entries(data);
	entries.forEach(function(i){
		

		let seminar_id=i[0];
		let data=i[1]

		let speaker_title=data['speaker_title'];
		let speaker=data['seminar_speaker'];
		let speaker_affil=data['speaker_affil'];
		let seminar_title=data['seminar_title'];
		let seminar_abstract=data['seminar_abstract'];

		let all_info=[speaker_title,speaker,speaker_affil,seminar_title,seminar_abstract].join(' ');
		index.add(seminar_id, all_info);

	});

	let fname=home_dir+'/Dropbox/websites/worldwideneuro.com/flexsearch_index.json';
	fs.writeFileSync(fname,index.export({index: true, doc: false}));
	
	//var response4 = await fetch('flexsearch_index.json');
	//var flexsearch_index = await response4.text();

}

request_data();