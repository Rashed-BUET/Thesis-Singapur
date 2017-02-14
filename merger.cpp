#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include <dirent.h>

using namespace std;

int main(int argc, char** argv){
	ofstream out;

	if (argc != 2)
	{
		cout<<"Please Enter The Parent Directory...."<<endl;
		return 0;
	}
    string STRING;
    STRING = string(argv[1]);
    string outname = STRING+"/merged";
    out.open(outname.c_str());
    ifstream infile;
    infile.open ("sample.txt");
    float sum = 0;
	int n = 0;
	string fileName;
	getline(infile,fileName);
	int size = atoi(fileName.c_str());
	while(size--){
		getline(infile,fileName);
		string directory = STRING+"/"+fileName;
    	//cout<<directory<<endl;
    	DIR *dir;
		struct dirent *ent;
		if ((dir = opendir(directory.c_str())) != NULL){
			sum = 0.0;
			n = 0;
			while((ent = readdir(dir)) != NULL){
				if (ent->d_name[strlen(ent->d_name)-1] != '~' && ent->d_name[0] != '.' && ent->d_name[0] != '_'){
					//printf("%s\n", ent->d_name );
					int t = directory.length();
					string sub = directory.substr(t-4);
					if (sub == "sumX")
					{
						ifstream infile1;
						//cout<<"..........."<<endl;
						string str = string(ent->d_name);
						str = directory+"/"+ent->d_name;
						infile1.open (str.c_str());
						//cout<<str<<endl;
						getline(infile1,str);
						//cout<<str<<endl;
						str = str.substr(str.find("\t")+1);
						sum += atof(str.c_str());
						getline(infile1,str);
						str = str.substr(str.find("\t")+1);
						//cout<<str<<endl;
						n += atoi(str.c_str());
						//cout<<n;
						
						infile1.close();
					}
					else 
					{
						ifstream infile1;
						//cout<<"..........."<<endl;
						string str = string(ent->d_name);
						str = directory+"/"+ent->d_name;
						infile1.open (str.c_str());
						//cout<<str<<endl;
						getline(infile1,str);
						//cout<<str<<endl;
						str = str.substr(str.find("\t")+1);
						sum += atof(str.c_str());
						infile1.close();
					}
				}
			}
			int t = directory.length();
			if(directory.substr(t-4) == "sumX"){
				cout<<"n\t"<<n<<endl<<fileName<<"\t"<<sum<<endl;
				out<<"n\t"<<n<<endl<<fileName<<"\t"<<sum<<endl;
			}
			else {
				cout<<fileName<<"\t"<<sum<<endl;
				out<<fileName<<"\t"<<sum<<endl;
			}
			
		}
	}
	infile.close();
	/*DIR *dir;
	struct dirent *ent;
	float sum = 0;
	int n = 0;
	if ((dir = opendir("sumX")) != NULL)
	{
		
		while((ent = readdir(dir)) != NULL){
			
			if (ent->d_name[strlen(ent->d_name)-1] != '~' && ent->d_name[0] != '.')
			{
				printf("%s\n", ent->d_name );
				string str;
    			str = string(ent->d_name);
    			str = "sumX/"+str;
    			//cout<<str<<endl;
				infile.open (str.c_str());
				getline(infile,str);
				//cout<<str<<endl;
				str = str.substr(str.find("\t")+1);
				sum += atof(str.c_str());
				getline(infile,str);
				str = str.substr(str.find("\t")+1);
				//cout<<str<<endl;
				n += atoi(str.c_str());

				
				infile.close();
			}
			
		}
	}
	cout<<sum<<endl<<n<<endl;
	*/

	return 0;
}

