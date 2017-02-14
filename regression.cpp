#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>
using namespace std;

int main (int argc, char** argv)
{
	ifstream in;
	string STRING;
    STRING = string(argv[1]);
    string filename = STRING+"/merged";
	in.open(filename.c_str());

	getline(in,STRING);
	int n = atoi((STRING.substr(STRING.find("\t")+1)).c_str());
	//cout<<n<<endl;
	getline(in,STRING);
	float sumx = atof((STRING.substr(STRING.find("\t")+1)).c_str());
        //cout<<sumx;
	getline(in,STRING);
	float sumxSquare = atof((STRING.substr(STRING.find("\t")+1)).c_str());
	cout<<sumxSquare<<endl;
	getline(in,STRING);
	float sumxCube = atof((STRING.substr(STRING.find("\t")+1)).c_str());

	getline(in,STRING);
	float sumxToFour = atof((STRING.substr(STRING.find("\t")+1)).c_str());

	getline(in,STRING);
	float sumy = atof((STRING.substr(STRING.find("\t")+1)).c_str());

	getline(in,STRING);
	float sumxy = atof((STRING.substr(STRING.find("\t")+1)).c_str());

	getline(in,STRING);
	float sumxSquarey = atof((STRING.substr(STRING.find("\t")+1)).c_str());
	
	//cout<<n<<x<<xSquare;	
	float a1 = (n*sumxy - sumx*sumy)/(n*sumxSquare-sumx*sumx);
	float a0 = sumy/n - a1*sumx/n;
	
	cout<<"\n\n\n\n\n\n\nLinear Regression\n"<<"Y(model) = "<<a0<<" + "<<a1<<"x"<<endl;
	
	float a = n, b = sumx, c = sumxSquare, d = -sumy,
	      l = sumx, m = sumxSquare, n0 = sumxCube, k = -sumxy,
	      p = sumxSquare, q = sumxCube, r = sumxToFour, s = -sumxSquarey,
	      D,x,y,z;
	
	
	D = (a*m*r+b*p*n0+c*l*q)-(a*n0*q+b*l*r+c*m*p);
 	x = ((b*r*k+c*m*s+d*n0*q)-(b*n0*s+c*q*k+d*m*r))/D;
 	y = ((a*n0*s+c*p*k+d*l*r)-(a*r*k+c*l*s+d*n0*p))/D;
 	z = ((a*q*k+b*l*s+d*m*p)-(a*m*s+b*p*k+d*l*q))/D;

	cout<<"\n\n\n\n\n\nPolynomial Regression\n"<<"Y(model) = "<<x<<" + "<<y<<"x"<<" + "<<z<<"x^2\n\n\n\n\n\n"<<endl;
	

	return 0;
}

